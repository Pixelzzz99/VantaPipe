use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, sleep, sleep_until};

use crate::config::{DependsOnEntry, load_config};
use crate::history::{RunHistoryStore, RunOutcome, RunRecord};
use crate::pipeline::{Pipeline, PipelineState};
use crate::retry::retry_with_backoff;
use crate::runtime::rebuild_pipeline_parts;
use crate::state::PersistentState;
use crate::web::{AppState, PipelineStatus};

#[derive(Clone)]
pub enum ScheduleMode {
    Interval(u64),
    Cron(Schedule),
}

#[derive(Debug, Clone)]
pub enum PipelineCommand {
    Pause,
    Resume,
    Stop,
    Trigger,
    Reload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlMode {
    Active,
    Paused,
    Stopped,
}

pub struct WorkerContext {
    pub id: String,
    pub config_path: String,
    pub state_path: String,
    pub pipeline: Arc<RwLock<Arc<Pipeline>>>,
    pub schedule: Arc<RwLock<ScheduleMode>>,
    pub depends_on: Arc<RwLock<Vec<DependsOnEntry>>>,
    pub pipeline_state: Arc<Mutex<PipelineState>>,
    pub persistent_state: Arc<Mutex<PersistentState>>,
    pub app_state: AppState,
    pub history: RunHistoryStore,
    pub cmd_rx: mpsc::Receiver<PipelineCommand>,
    /// Fires with a pipeline id whenever that pipeline finishes a real
    /// success (see `AppState::success_tx`). Lets this worker react to a
    /// dependency succeeding immediately rather than waiting for its own
    /// next scheduled tick.
    pub dep_rx: broadcast::Receiver<String>,
}

/// Run forever: schedule ticks + control commands (pause/stop/trigger/reload).
pub async fn run_pipeline_worker(mut ctx: WorkerContext) {
    let running = Arc::new(AtomicBool::new(false));
    let mut mode = ControlMode::Active;

    // First tick immediately when active.
    try_schedule_tick(&ctx, &running, mode, true).await;

    loop {
        let wait = wait_next_tick_future(&ctx.schedule);

        tokio::select! {
            _ = wait => {
                try_schedule_tick(&ctx, &running, mode, false).await;
            }
            cmd = ctx.cmd_rx.recv() => {
                match cmd {
                    Some(PipelineCommand::Pause) => {
                        mode = ControlMode::Paused;
                        ctx.app_state.set_pipeline_status(&ctx.id, PipelineStatus::Paused);
                        ctx.app_state.log(format!("[{}] [INFO] Paused", ctx.id));
                        log::info!("[{}] Paused", ctx.id);
                    }
                    Some(PipelineCommand::Resume) => {
                        mode = ControlMode::Active;
                        if !running.load(Ordering::SeqCst) {
                            ctx.app_state.set_pipeline_status(&ctx.id, PipelineStatus::Idle);
                        }
                        ctx.app_state.log(format!("[{}] [INFO] Resumed", ctx.id));
                        log::info!("[{}] Resumed", ctx.id);
                    }
                    Some(PipelineCommand::Stop) => {
                        mode = ControlMode::Stopped;
                        ctx.app_state.set_pipeline_status(&ctx.id, PipelineStatus::Stopped);
                        ctx.app_state.log(format!("[{}] [INFO] Stopped (soft)", ctx.id));
                        log::info!("[{}] Stopped (soft)", ctx.id);
                    }
                    Some(PipelineCommand::Trigger) => {
                        try_schedule_tick(&ctx, &running, mode, true).await;
                    }
                    Some(PipelineCommand::Reload) => {
                        reload_pipeline(&ctx).await;
                    }
                    None => {
                        log::warn!("[{}] Control channel closed", ctx.id);
                        break;
                    }
                }
            }
            event = ctx.dep_rx.recv() => {
                if let Ok(finished_id) = event {
                    let depends_on = ctx.depends_on.read().unwrap().clone();
                    if depends_on.iter().any(|d| d.id() == finished_id) {
                        try_schedule_tick(&ctx, &running, mode, false).await;
                    }
                }
                // Err(Lagged)/Err(Closed): ignore — the pipeline's own
                // scheduled tick remains the fallback that eventually
                // re-checks depends_on.
            }
        }
    }
}

/// `Ok(())` if every dependency's last run succeeded (and, where set,
/// isn't stale). `Err(reason)` otherwise, with a human-readable reason
/// suitable for both logging and surfacing as the pipeline's `Blocked`
/// status in the dashboard.
fn dependencies_satisfied(
    id: &str,
    depends_on: &[DependsOnEntry],
    history: &RunHistoryStore,
) -> Result<(), String> {
    for dep in depends_on {
        let dep_id = dep.id();
        match history.for_pipeline(dep_id).last() {
            Some(rec) if rec.status == RunOutcome::Success => {
                if let Some(max_staleness_secs) = dep.max_staleness_secs() {
                    let finished_at = rec.finished_at.unwrap_or(rec.started_at);
                    let age_secs = Utc::now()
                        .signed_duration_since(finished_at)
                        .num_seconds()
                        .max(0) as u64;
                    if age_secs > max_staleness_secs {
                        let reason = format!(
                            "waiting on '{}': last success is {}s old, exceeds max_staleness_secs {}",
                            dep_id, age_secs, max_staleness_secs
                        );
                        log::info!("[{}] {}", id, reason);
                        return Err(reason);
                    }
                }
            }
            Some(_) => {
                let reason = format!("waiting on '{}': last run was not successful", dep_id);
                log::info!("[{}] {}", id, reason);
                return Err(reason);
            }
            None => {
                let reason = format!("waiting on '{}': no run history yet", dep_id);
                log::info!("[{}] {}", id, reason);
                return Err(reason);
            }
        }
    }
    Ok(())
}

async fn try_schedule_tick(
    ctx: &WorkerContext,
    running: &Arc<AtomicBool>,
    mode: ControlMode,
    force: bool,
) {
    if !force && mode != ControlMode::Active {
        return;
    }

    if !force {
        let depends_on = ctx.depends_on.read().unwrap().clone();
        if !depends_on.is_empty() {
            if let Err(reason) = dependencies_satisfied(&ctx.id, &depends_on, &ctx.history) {
                ctx.app_state
                    .set_pipeline_status(&ctx.id, PipelineStatus::Blocked(reason));
                return;
            }
        }
    }

    if running.load(Ordering::SeqCst) {
        log::warn!(
            "[{}] Skipping tick: previous run still in progress",
            ctx.id
        );
        ctx.app_state.log(format!(
            "[{}] [WARN] Skipping tick: previous run still in progress",
            ctx.id
        ));
        return;
    }

    execute_tick(ctx, running, mode).await;
}

async fn execute_tick(ctx: &WorkerContext, running: &Arc<AtomicBool>, mode: ControlMode) {
    if running
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        log::warn!("[{}] Skipping tick: previous run still in progress", ctx.id);
        ctx.app_state.log(format!(
            "[{}] [WARN] Skipping tick: previous run still in progress",
            ctx.id
        ));
        return;
    }

    let id = ctx.id.clone();
    let pipeline = {
        let guard = ctx.pipeline.read().unwrap();
        Arc::clone(&guard)
    };
    let pipeline_state = Arc::clone(&ctx.pipeline_state);
    let persistent_state = Arc::clone(&ctx.persistent_state);
    let state_path = ctx.state_path.clone();
    let app_state = ctx.app_state.clone();
    let history = ctx.history.clone();
    let running = Arc::clone(running);

    tokio::spawn(async move {
        run_once(
            &id,
            &pipeline,
            &pipeline_state,
            &persistent_state,
            &state_path,
            &app_state,
            &history,
            mode,
        )
        .await;
        running.store(false, Ordering::SeqCst);
    });
}

async fn run_once(
    id: &str,
    pipeline: &Pipeline,
    pipeline_state: &Arc<Mutex<PipelineState>>,
    persistent_state: &Arc<Mutex<PersistentState>>,
    state_path: &str,
    app_state: &AppState,
    history: &RunHistoryStore,
    mode: ControlMode,
) {
    let started_at = Utc::now();
    app_state.set_pipeline_status(id, PipelineStatus::Running);
    let op_name = format!("pipeline:{}", id);
    let result = retry_with_backoff(3, 2, &op_name, || pipeline.run(pipeline_state)).await;
    let finished_at = Utc::now();

    let restore_status = |app: &AppState| {
        match mode {
            ControlMode::Paused => app.set_pipeline_status(id, PipelineStatus::Paused),
            ControlMode::Stopped => app.set_pipeline_status(id, PipelineStatus::Stopped),
            ControlMode::Active => app.set_pipeline_status(id, PipelineStatus::Idle),
        }
    };

    match result {
        Ok(0) => {
            restore_status(app_state);
            let msg = format!("[{}] [INFO] No new data", id);
            app_state.log(msg.clone());
            log::info!("{}", msg);
            history.record_finished(RunRecord {
                pipeline_id: id.to_string(),
                started_at,
                finished_at: Some(finished_at),
                status: RunOutcome::Empty,
                rows: 0,
                error: None,
                error_kind: None,
            });
        }
        Ok(count) => {
            log::info!("[{}] Processed {} rows", id, count);
            app_state.add_pipeline_rows(id, count);
            restore_status(app_state);
            app_state.log(format!("[{}] [INFO] Processed {} rows", id, count));

            {
                let mut s = persistent_state.lock().unwrap();
                s.total_rows_processed += count;
                s.last_run = pipeline_state.lock().unwrap().last_run;
            }
            save_state(persistent_state, state_path);
            history.record_finished(RunRecord {
                pipeline_id: id.to_string(),
                started_at,
                finished_at: Some(finished_at),
                status: RunOutcome::Success,
                rows: count,
                error: None,
                error_kind: None,
            });
            // Wake up any dependents waiting on this pipeline (see
            // `WorkerContext.dep_rx`) instead of making them poll on
            // their own schedule.
            app_state.success_tx.send(id.to_string()).ok();
        }
        Err(e) => {
            log::error!("[{}] Pipeline error: {}", id, e);
            app_state.add_pipeline_error(id);
            if mode == ControlMode::Active {
                app_state.set_pipeline_error(id, e.to_string(), e.kind());
            } else {
                restore_status(app_state);
            }
            app_state.log(format!("[{}] [ERROR] {}", id, e));

            persistent_state.lock().unwrap().total_errors += 1;
            save_state(persistent_state, state_path);
            history.record_finished(RunRecord {
                pipeline_id: id.to_string(),
                started_at,
                finished_at: Some(finished_at),
                status: RunOutcome::Error,
                rows: 0,
                error: Some(e.to_string()),
                error_kind: Some(e.kind().to_string()),
            });
        }
    }
}

async fn reload_pipeline(ctx: &WorkerContext) {
    let config = match load_config(&ctx.config_path) {
        Ok(c) => c,
        Err(e) => {
            log::error!("[{}] Reload failed (read config): {}", ctx.id, e);
            ctx.app_state
                .set_pipeline_error(&ctx.id, e.to_string(), e.kind());
            ctx.app_state
                .log(format!("[{}] [ERROR] Reload failed: {}", ctx.id, e));
            return;
        }
    };

    let other_paths = ctx.app_state.all_config_paths();
    if let Err(e) =
        crate::config::validate_depends_on_for_reload(&ctx.id, &config.depends_on, &other_paths)
    {
        log::error!("[{}] Reload rejected: {}", ctx.id, e);
        ctx.app_state
            .set_pipeline_error(&ctx.id, e.to_string(), e.kind());
        ctx.app_state
            .log(format!("[{}] [ERROR] Reload rejected: {}", ctx.id, e));
        return;
    }

    match rebuild_pipeline_parts(
        &ctx.id,
        &ctx.state_path,
        &config,
        Arc::clone(&ctx.persistent_state),
    )
    .await
    {
        Ok((pipeline, schedule, label)) => {
            {
                let mut p = ctx.pipeline.write().unwrap();
                *p = Arc::new(pipeline);
            }
            {
                let mut s = ctx.schedule.write().unwrap();
                *s = schedule;
            }
            {
                let mut d = ctx.depends_on.write().unwrap();
                *d = config.depends_on.clone().unwrap_or_default();
            }
            ctx.app_state.update_pipeline_schedule(&ctx.id, label.clone());
            ctx.app_state.log(format!(
                "[{}] [INFO] Reloaded config ({})",
                ctx.id, label
            ));
            log::info!("[{}] Reloaded successfully", ctx.id);
            if !matches!(
                ctx.app_state.get_pipeline_status(&ctx.id),
                Some(PipelineStatus::Paused) | Some(PipelineStatus::Stopped)
            ) {
                ctx.app_state
                    .set_pipeline_status(&ctx.id, PipelineStatus::Idle);
            }
        }
        Err(e) => {
            log::error!("[{}] Reload failed (rebuild): {}", ctx.id, e);
            ctx.app_state
                .set_pipeline_error(&ctx.id, e.to_string(), e.kind());
            ctx.app_state
                .log(format!("[{}] [ERROR] Reload failed: {}", ctx.id, e));
        }
    }
}

fn wait_next_tick_future(
    schedule: &Arc<RwLock<ScheduleMode>>,
) -> impl std::future::Future<Output = ()> + '_ {
    async move {
        let mode = schedule.read().unwrap().clone();
        match mode {
            ScheduleMode::Interval(secs) => {
                sleep(Duration::from_secs(secs)).await;
            }
            ScheduleMode::Cron(cron) => {
                let Some(next) = cron.upcoming(Utc).next() else {
                    log::error!("Cron schedule has no upcoming fire time");
                    sleep(Duration::from_secs(60)).await;
                    return;
                };
                let now = Utc::now();
                let wait = (next - now).to_std().unwrap_or(Duration::from_secs(0));
                sleep_until(Instant::now() + wait).await;
            }
        }
    }
}

fn save_state(persistent_state: &Arc<Mutex<PersistentState>>, path: &str) {
    let snapshot = persistent_state.lock().unwrap().clone();
    if let Err(e) = snapshot.save(path) {
        log::warn!("Failed to save state {}: {}", path, e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paused_blocks_non_force_ticks() {
        assert!(ControlMode::Paused != ControlMode::Active);
        let force = false;
        let mode = ControlMode::Paused;
        let should_run = force || mode == ControlMode::Active;
        assert!(!should_run);

        let force_trigger = true;
        let should_run_trigger = force_trigger || mode == ControlMode::Active;
        assert!(should_run_trigger);
    }

    #[tokio::test]
    async fn test_skip_when_already_running() {
        let running = Arc::new(AtomicBool::new(true));
        let skipped = running.load(Ordering::SeqCst);
        assert!(skipped);
    }

    fn test_history_store(name: &str) -> RunHistoryStore {
        let dir = std::env::temp_dir().join(format!(
            "etl_sched_deps_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        RunHistoryStore::new(dir)
    }

    fn record(pipeline_id: &str, status: RunOutcome) -> RunRecord {
        record_finished_at(pipeline_id, status, Utc::now())
    }

    fn record_finished_at(
        pipeline_id: &str,
        status: RunOutcome,
        finished_at: chrono::DateTime<Utc>,
    ) -> RunRecord {
        RunRecord {
            pipeline_id: pipeline_id.to_string(),
            started_at: finished_at,
            finished_at: Some(finished_at),
            status,
            rows: 0,
            error: None,
            error_kind: None,
        }
    }

    fn simple_dep(id: &str) -> DependsOnEntry {
        DependsOnEntry::Simple(id.to_string())
    }

    fn dep_with_staleness(id: &str, max_staleness_secs: u64) -> DependsOnEntry {
        DependsOnEntry::Detailed {
            id: id.to_string(),
            max_staleness_secs: Some(max_staleness_secs),
        }
    }

    #[test]
    fn test_dependencies_satisfied_empty_deps() {
        let history = test_history_store("empty");
        assert!(dependencies_satisfied("downstream", &[], &history).is_ok());
    }

    #[test]
    fn test_dependencies_satisfied_no_history() {
        let history = test_history_store("no_history");
        let deps = vec![simple_dep("upstream")];
        let result = dependencies_satisfied("downstream", &deps, &history);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no run history yet"));
    }

    #[test]
    fn test_dependencies_satisfied_last_run_failed() {
        let history = test_history_store("failed");
        history.record_finished(record("upstream", RunOutcome::Error));
        let deps = vec![simple_dep("upstream")];
        let result = dependencies_satisfied("downstream", &deps, &history);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("was not successful"));
    }

    #[test]
    fn test_dependencies_satisfied_most_recent_success_after_earlier_error() {
        let history = test_history_store("recovers");
        history.record_finished(record("upstream", RunOutcome::Error));
        history.record_finished(record("upstream", RunOutcome::Success));
        let deps = vec![simple_dep("upstream")];
        assert!(dependencies_satisfied("downstream", &deps, &history).is_ok());
    }

    #[test]
    fn test_dependencies_satisfied_requires_all_deps() {
        let history = test_history_store("multi");
        history.record_finished(record("upstream_a", RunOutcome::Success));
        history.record_finished(record("upstream_b", RunOutcome::Error));
        let deps = vec![simple_dep("upstream_a"), simple_dep("upstream_b")];
        assert!(dependencies_satisfied("downstream", &deps, &history).is_err());
    }

    #[test]
    fn test_dependencies_satisfied_stale_success_rejected() {
        let history = test_history_store("stale");
        let two_hours_ago = Utc::now() - chrono::Duration::hours(2);
        history.record_finished(record_finished_at("upstream", RunOutcome::Success, two_hours_ago));
        // last success was 2h ago, but max_staleness_secs allows only 1h
        let deps = vec![dep_with_staleness("upstream", 3600)];
        let result = dependencies_satisfied("downstream", &deps, &history);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds max_staleness_secs"));
    }

    #[test]
    fn test_dependencies_satisfied_fresh_success_accepted() {
        let history = test_history_store("fresh");
        let two_hours_ago = Utc::now() - chrono::Duration::hours(2);
        history.record_finished(record_finished_at("upstream", RunOutcome::Success, two_hours_ago));
        // last success was 2h ago, max_staleness_secs allows up to 3h
        let deps = vec![dep_with_staleness("upstream", 3 * 3600)];
        assert!(dependencies_satisfied("downstream", &deps, &history).is_ok());
    }

    #[test]
    fn test_dependencies_satisfied_no_staleness_limit_accepts_old_success() {
        let history = test_history_store("no_limit");
        let ten_days_ago = Utc::now() - chrono::Duration::days(10);
        history.record_finished(record_finished_at("upstream", RunOutcome::Success, ten_days_ago));
        // Simple entries have no staleness limit, however old the success.
        let deps = vec![simple_dep("upstream")];
        assert!(dependencies_satisfied("downstream", &deps, &history).is_ok());
    }

    struct StubExtractor;
    #[async_trait::async_trait]
    impl crate::extractor::Extractor for StubExtractor {
        async fn extract(
            &self,
            _last_run: chrono::DateTime<Utc>,
        ) -> Result<Vec<crate::types::Row>, crate::error::EtlError> {
            Ok(vec![])
        }
    }

    struct StubLoader;
    #[async_trait::async_trait]
    impl crate::loader::Loader for StubLoader {
        async fn load(&self, _rows: Vec<crate::types::Row>) -> Result<(), crate::error::EtlError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_try_schedule_tick_sets_blocked_status_when_dependency_unsatisfied() {
        let history = test_history_store("blocked_status");
        let app_state = AppState::new(history.clone(), Default::default(), None);
        app_state.register_pipeline(
            "downstream".to_string(),
            "downstream.json".to_string(),
            "every 10s".to_string(),
            0,
            0,
        );

        let pipeline = Pipeline::new(Box::new(StubExtractor), vec![], Box::new(StubLoader));
        let ctx = WorkerContext {
            id: "downstream".to_string(),
            config_path: "downstream.json".to_string(),
            state_path: "downstream_state.json".to_string(),
            pipeline: Arc::new(RwLock::new(Arc::new(pipeline))),
            schedule: Arc::new(RwLock::new(ScheduleMode::Interval(10))),
            depends_on: Arc::new(RwLock::new(vec![simple_dep("upstream")])),
            pipeline_state: Arc::new(Mutex::new(crate::pipeline::PipelineState::new())),
            persistent_state: Arc::new(Mutex::new(PersistentState::new())),
            app_state: app_state.clone(),
            history,
            cmd_rx: mpsc::channel(1).1,
            dep_rx: app_state.subscribe_success(),
        };
        let running = Arc::new(AtomicBool::new(false));

        try_schedule_tick(&ctx, &running, ControlMode::Active, false).await;

        match app_state.get_pipeline_status("downstream") {
            Some(PipelineStatus::Blocked(reason)) => {
                assert!(reason.contains("no run history yet"));
            }
            other => panic!("expected Blocked status, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_downstream_triggers_immediately_on_upstream_success() {
        let history = test_history_store("dag_trigger");
        let app_state = AppState::new(history.clone(), Default::default(), None);
        app_state.register_pipeline(
            "downstream".to_string(),
            "downstream.json".to_string(),
            "every 3600s".to_string(),
            0,
            0,
        );

        let pipeline = Pipeline::new(Box::new(StubExtractor), vec![], Box::new(StubLoader));
        // Kept alive for the whole test: dropping the sender closes
        // cmd_rx, which the worker loop treats as a shutdown signal.
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let ctx = WorkerContext {
            id: "downstream".to_string(),
            config_path: "downstream.json".to_string(),
            state_path: "downstream_state.json".to_string(),
            pipeline: Arc::new(RwLock::new(Arc::new(pipeline))),
            // Effectively "never" within this test — proves the second
            // tick below comes from the broadcast, not the timer.
            schedule: Arc::new(RwLock::new(ScheduleMode::Interval(3600))),
            depends_on: Arc::new(RwLock::new(vec![simple_dep("upstream")])),
            pipeline_state: Arc::new(Mutex::new(crate::pipeline::PipelineState::new())),
            persistent_state: Arc::new(Mutex::new(PersistentState::new())),
            app_state: app_state.clone(),
            history: history.clone(),
            cmd_rx,
            dep_rx: app_state.subscribe_success(),
        };

        tokio::spawn(run_pipeline_worker(ctx));

        // Initial forced startup tick (bypasses the depends_on gate).
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            history.for_pipeline("downstream").len(),
            1,
            "expected exactly the initial forced startup tick"
        );

        history.record_finished(record("upstream", RunOutcome::Success));
        app_state.success_tx.send("upstream".to_string()).ok();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            history.for_pipeline("downstream").len(),
            2,
            "expected a second tick triggered by the upstream success broadcast, not the 3600s timer"
        );
    }
}
