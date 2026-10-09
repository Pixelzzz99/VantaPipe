# Плейбук ручного тестирования

Всё, что было построено в этом раунде: `error_kind`, `depends_on` (+ окно
устаревания + детекция циклов + валидация при reload), HTTP Basic Auth,
фикс двойного reload, чистка мёртвого кода, динамическая регистрация
пайплайнов на лету. Прогоняйте после любых изменений в `scheduler.rs`,
`config.rs`, `auth.rs`, `web/*`, `watcher.rs` или `registry.rs`.

Каждый раздел самодостаточен — можно прыгать по разделам, если правили
только одну область. Команды подразумевают, что вы находитесь в корне
репозитория (`/Users/sherzod/RustForFun/etl-engine`).

---

## 0. Подготовка

```bash
cargo build
cargo test          # ожидается: 0 предупреждений, все тесты зелёные
docker compose up -d postgres
```

Держите отдельную scratch-директорию для одноразовых конфигов пайплайнов,
чтобы не трогать файлы, отслеживаемые git:

```bash
mkdir -p /tmp/etl_playbook
```

Выберите свободный порт для ручных запусков — `3000` и `3001` на macOS часто
заняты другими локальными сервисами (Node, Docker Desktop). Проверьте заранее:

```bash
lsof -i :3050 || echo "3050 свободен"
```

Ниже используется `3050`, если не указано иное.

---

## 1. Smoke-тест — один пайплайн, дашборд загружается

```bash
cd /Users/sherzod/RustForFun/etl-engine
rm -f etl_state.json
RUST_LOG=info cargo run -- config/pipeline_csv.json etl_state.json 3050
```

- [ ] В логе `Web UI running at http://localhost:3050` без паник
- [ ] В логе `API authentication is disabled — set ETL_AUTH_USER and ETL_AUTH_PASS ...` (ожидаемо — переменные окружения не заданы)
- [ ] Открыть `http://localhost:3050` — дашборд загружается, строка пайплайна `pipeline_csv`, статус `IDLE`
- [ ] `curl -s http://localhost:3050/api/status | python3 -m json.tool` возвращает валидный JSON, у пайплайна есть поле `error_kind: null`

Ctrl-C, когда раздел пройден.

---

## 2. Управление: Pause / Resume / Stop / Run

Сервер из §1 всё ещё запущен:

```bash
curl -s -X POST http://localhost:3050/api/pipelines/pipeline_csv/pause
curl -s http://localhost:3050/api/status | python3 -c "import json,sys; print(json.load(sys.stdin)['pipelines'][0]['status'])"
# ожидается: "paused"

curl -s -X POST http://localhost:3050/api/pipelines/pipeline_csv/resume
curl -s -X POST http://localhost:3050/api/pipelines/pipeline_csv/stop
curl -s -X POST http://localhost:3050/api/pipelines/pipeline_csv/run
```

- [ ] Каждая команда меняет статус соответственно (`paused` → `idle` → `stopped`)
- [ ] `run` добавляет новую запись в `curl -s http://localhost:3050/api/history | python3 -m json.tool` в течение пары секунд
- [ ] То же самое через кнопки дашборда (Pause/Resume/Stop/Run) — цвет точки статуса меняется (синий/жёлтый/оранжевый/зелёный)

---

## 3. JSON-редактор + hot reload (без двойного срабатывания)

```bash
curl -s http://localhost:3050/api/pipelines/pipeline_csv/config | python3 -c "import json,sys; print(json.load(sys.stdin)['config'])"
```

Меняем `poll_interval_secs` через API (имитирует "Save & reload" в дашборде):

```bash
curl -s -X PUT http://localhost:3050/api/pipelines/pipeline_csv/config \
  -d '{"source":{"type":"csv","watch_dir":"data/watched","processed_dir":"data/processed","delimiter":",","chunk_size":10000,"poll_interval_secs":15},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"imported_data","unique_key":null}}'
```

- [ ] Ответ `{"id":"pipeline_csv","ok":true}`
- [ ] Колонка **Schedule** в дашборде обновляется на `every 15s` (перезагрузить страницу или дождаться следующего опроса статуса)
- [ ] В логе сервера считаем строки `Reloaded successfully` сразу после PUT — должна быть **ровно 1**, не 2 (это был баг двойного reload):
  ```bash
  grep -c "Reloaded successfully" <вывод лога с момента PUT>
  ```

Откатываем:

```bash
curl -s -X PUT http://localhost:3050/api/pipelines/pipeline_csv/config \
  -d '{"source":{"type":"csv","watch_dir":"data/watched","processed_dir":"data/processed","delimiter":",","chunk_size":10000,"poll_interval_secs":10},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"imported_data","unique_key":null}}'
```

- [ ] `git diff config/pipeline_csv.json` не показывает остаточных изменений

---

## 4. Видимость `error_kind` (реальный обрыв соединения)

Свежая настройка, отдельный пайплайн, указывающий на недоступный ClickHouse:

```bash
cat > /tmp/etl_playbook/err.json <<'EOF'
{
  "id": "err_demo",
  "source": {"type": "clickhouse", "host": "http://localhost:1", "database": "default", "query": "SELECT 1", "poll_interval_secs": 30},
  "transforms": [],
  "destination": {"type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "err_tbl", "unique_key": null}
}
EOF
rm -f /tmp/etl_playbook/state.json
RUST_LOG=info cargo run -- /tmp/etl_playbook/err.json /tmp/etl_playbook/state.json 3050
```

В другом терминале запускаем вручную и ждём ~10с, пока не истощатся все 3 попытки retry:

```bash
curl -s -X POST http://localhost:3050/api/pipelines/err_demo/run
sleep 10
curl -s http://localhost:3050/api/status | python3 -m json.tool
```

- [ ] `status` равен `{"error": "Connection error: ..."}`
- [ ] `error_kind` равен `"connection"` (не просто зашит в текст сообщения)
- [ ] Дашборд показывает красную точку, `ERROR` и небольшой бейдж `CONNECTION` рядом со статусом, ниже — усечённое сообщение

Ctrl-C, очистка: `rm -rf /tmp/etl_playbook/*`.

---

## 5. `depends_on` — базовый сценарий satisfied / blocked

```bash
mkdir -p /tmp/etl_playbook/watched_a /tmp/etl_playbook/processed_a /tmp/etl_playbook/watched_b /tmp/etl_playbook/processed_b
cat > /tmp/etl_playbook/a.json <<'EOF'
{
  "id": "a",
  "source": {"type": "csv", "watch_dir": "/tmp/etl_playbook/watched_a", "processed_dir": "/tmp/etl_playbook/processed_a", "poll_interval_secs": 20},
  "transforms": [],
  "destination": {"type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "a_tbl", "unique_key": null}
}
EOF
cat > /tmp/etl_playbook/b.json <<'EOF'
{
  "id": "b",
  "depends_on": ["a"],
  "source": {"type": "csv", "watch_dir": "/tmp/etl_playbook/watched_b", "processed_dir": "/tmp/etl_playbook/processed_b", "poll_interval_secs": 20},
  "transforms": [],
  "destination": {"type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "b_tbl", "unique_key": null}
}
EOF
RUST_LOG=info cargo run -- /tmp/etl_playbook /tmp/etl_playbook/state 3050
```

Оба пайплайна форсированно запускаются один раз при старте (пусто, CSV-данных ещё нет).
Ждём ~20с первого *запланированного* (не форсированного) тика `b`:

- [ ] В логе `[b] waiting on 'a': last run was not successful` (у его зависимости единственный запуск пока был `Empty`, а не `Success`)
- [ ] `curl -s http://localhost:3050/api/status | python3 -m json.tool` показывает статус `b` как `{"blocked": "waiting on 'a': last run was not successful"}`
- [ ] Дашборд показывает `b` с фиолетовой точкой и `BLOCKED`, текст причины под статусом

Теперь даём `a` реальные данные, чтобы он мог успешно завершиться, и проверяем `b`:

```bash
printf 'id,name\n1,alice\n' > /tmp/etl_playbook/watched_a/data.csv
curl -s -X POST http://localhost:3050/api/pipelines/a/run
sleep 3
curl -s http://localhost:3050/api/pipelines/a/history | python3 -m json.tool   # ожидается запись "success"
```

- [ ] В течение 20с (следующий запланированный тик `b`) `b` выходит из `BLOCKED` и запускается нормально

Ctrl-C, очистка.

---

## 6. `depends_on` — окно устаревания (staleness)

Та же настройка с двумя пайплайнами, что в §5, но `b.json` меняем перед стартом:

```bash
cat > /tmp/etl_playbook/b.json <<'EOF'
{
  "id": "b",
  "depends_on": [{"id": "a", "max_staleness_secs": 5}],
  "source": {"type": "csv", "watch_dir": "/tmp/etl_playbook/watched_b", "processed_dir": "/tmp/etl_playbook/processed_b", "poll_interval_secs": 20},
  "transforms": [],
  "destination": {"type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "b_tbl", "unique_key": null}
}
EOF
```

- [ ] Дождаться успеха `a` один раз (как в §5), затем подождать **больше 5 секунд** перед следующим тиком `b` — в логе должно появиться `last success is Ns old, exceeds max_staleness_secs 5`, и `b` остаётся `BLOCKED`, даже несмотря на то что последний запуск `a` *был* успешным
- [ ] Перезапустить `a` прямо перед следующим тиком `b` — `b` должен продолжить работу (достаточно свежо)

---

## 7. `depends_on` — валидация (неизвестный id / self-dep / цикл)

Сервер из §5/§6 запущен (или свежий, с `a`/`b`):

**Неизвестный id, через API:**
```bash
curl -s -w "\nHTTP:%{http_code}\n" -X PUT http://localhost:3050/api/pipelines/a/config \
  -d '{"id":"a","depends_on":["does_not_exist"],"source":{"type":"csv","watch_dir":"/tmp/etl_playbook/watched_a","processed_dir":"/tmp/etl_playbook/processed_a","poll_interval_secs":20},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"a_tbl","unique_key":null}}'
```
- [ ] `400`, тело содержит `unknown pipeline id`
- [ ] `cat /tmp/etl_playbook/a.json` — файл на диске **не изменился** (отклонено до записи)

**Цикл, через API:**
```bash
curl -s -w "\nHTTP:%{http_code}\n" -X PUT http://localhost:3050/api/pipelines/a/config \
  -d '{"id":"a","depends_on":["b"],"source":{"type":"csv","watch_dir":"/tmp/etl_playbook/watched_a","processed_dir":"/tmp/etl_playbook/processed_a","poll_interval_secs":20},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"a_tbl","unique_key":null}}'
```
- [ ] `400`, тело содержит `Dependency cycle detected: b -> a -> b`

**Цикл, через прямое редактирование файла на диске (в обход API — это путь, который раньше был не защищён):**
```bash
cat > /tmp/etl_playbook/a.json.tmp <<'EOF'
{
  "id": "a",
  "depends_on": ["b"],
  "source": {"type": "csv", "watch_dir": "/tmp/etl_playbook/watched_a", "processed_dir": "/tmp/etl_playbook/processed_a", "poll_interval_secs": 20},
  "transforms": [],
  "destination": {"type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "a_tbl", "unique_key": null}
}
EOF
mv /tmp/etl_playbook/a.json.tmp /tmp/etl_playbook/a.json   # атомарный rename — watcher не увидит наполовину записанный файл
```
- [ ] В логе `[a] Reload rejected: Config error: Dependency cycle detected: b -> a -> b`
- [ ] `a` продолжает работать на старом (валидном) конфиге — проверить `/api/status`, статус не завис в цикле ошибок/креша

**Self-dependency** — та же идея, `"depends_on": ["a"]` в `a.json` → ожидается `cannot depend on itself`, через API или через файл.

Ctrl-C, очистка: `rm -rf /tmp/etl_playbook`.

---

## 8. Авторизация

**Выключена (по умолчанию):**
```bash
rm -f etl_state.json
RUST_LOG=info cargo run -- config/pipeline_csv.json etl_state.json 3050
```
- [ ] В логе при старте: `API authentication is disabled — set ETL_AUTH_USER and ETL_AUTH_PASS ...`
- [ ] `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3050/api/status` → `200`

Ctrl-C.

**Включена:**
```bash
rm -f etl_state.json
ETL_AUTH_USER=admin ETL_AUTH_PASS=s3cr3t RUST_LOG=info \
  cargo run -- config/pipeline_csv.json etl_state.json 3050
```
- [ ] В логе при старте: `API authentication enabled for user 'admin'`
- [ ] Без креденшелов: `curl -s -i http://localhost:3050/api/status | head -3` → `401`, заголовок `www-authenticate: Basic realm="etl-engine"`
- [ ] Неверный пароль: `curl -s -o /dev/null -w "%{http_code}\n" -u admin:wrong http://localhost:3050/api/status` → `401`
- [ ] Верные креды: `curl -s -o /dev/null -w "%{http_code}\n" -u admin:s3cr3t http://localhost:3050/api/status` → `200`
- [ ] Корень дашборда тоже защищён: `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3050/` → `401` без креденшелов
- [ ] Открыть `http://localhost:3050` в настоящем браузере (не curl) — появляется нативное окно логина; неверные креды переспрашивают, верные — грузят дашборд и остаются залогинены для последующих кликов

Ctrl-C.

**Частичная конфигурация (fail-fast):**
```bash
ETL_AUTH_USER=admin RUST_LOG=info cargo run -- config/pipeline_csv.json etl_state.json 3050
```
- [ ] Процесс сразу завершается с `Invalid auth configuration: ETL_AUTH_USER is set but ETL_AUTH_PASS is not — set both or neither`, веб-сервер **не** стартует

---

## 9. Динамическая регистрация пайплайнов (directory mode)

**Старт с пустой директорией:**

```bash
rm -rf /tmp/etl_playbook && mkdir -p /tmp/etl_playbook/config /tmp/etl_playbook/state /tmp/etl_playbook/watched /tmp/etl_playbook/processed
RUST_LOG=info cargo run -- /tmp/etl_playbook/config /tmp/etl_playbook/state 3050
```
- [ ] Лог: `ETL Engine started with 0 pipeline(s)` — процесс не падает на пустой директории
- [ ] Лог: `Watching configs in /tmp/etl_playbook/config` — директория наблюдается, даже когда в ней пусто

**Регистрация через `POST /api/pipelines`:**
```bash
curl -s -w "\nHTTP:%{http_code}\n" -X POST http://localhost:3050/api/pipelines \
  -d '{"id":"via_api","source":{"type":"csv","watch_dir":"/tmp/etl_playbook/watched","processed_dir":"/tmp/etl_playbook/processed","poll_interval_secs":15},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"via_api_tbl","unique_key":null}}'
```
- [ ] `201`, тело `{"ok":true,"id":"via_api"}`
- [ ] `curl -s http://localhost:3050/api/status | python3 -m json.tool` показывает `via_api` со статусом `idle` — без перезапуска процесса
- [ ] `cat /tmp/etl_playbook/config/via_api.json` — файл реально на диске

**Регистрация просто через файл (без API) — основной сценарий "запустили → создали json → пошли дальше":**
```bash
cat > /tmp/etl_playbook/config/via_file.json.tmp <<'EOF'
{
  "id": "via_file",
  "source": {"type": "csv", "watch_dir": "/tmp/etl_playbook/watched", "processed_dir": "/tmp/etl_playbook/processed", "poll_interval_secs": 15},
  "transforms": [],
  "destination": {"type": "postgres", "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb", "table": "via_file_tbl", "unique_key": null}
}
EOF
mv /tmp/etl_playbook/config/via_file.json.tmp /tmp/etl_playbook/config/via_file.json   # атомарный rename
sleep 2
```
- [ ] Лог: `[via_file] New config file detected: ...`, затем `[via_file] Schedule: every 15s ...`
- [ ] `/api/status` показывает `via_file` — воркер поднялся сам, без единого API-вызова

**Негативные кейсы (не должны ронять процесс):**
```bash
# дубликат id через API
curl -s -w "\nHTTP:%{http_code}\n" -X POST http://localhost:3050/api/pipelines \
  -d '{"id":"via_api","source":{"type":"csv","watch_dir":"/tmp/etl_playbook/watched","processed_dir":"/tmp/etl_playbook/processed","poll_interval_secs":15},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"x","unique_key":null}}'
# без "id" в теле
curl -s -w "\nHTTP:%{http_code}\n" -X POST http://localhost:3050/api/pipelines \
  -d '{"source":{"type":"csv","watch_dir":"/tmp/etl_playbook/watched","processed_dir":"/tmp/etl_playbook/processed","poll_interval_secs":15},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"x","unique_key":null}}'
# depends_on на несуществующий id
curl -s -w "\nHTTP:%{http_code}\n" -X POST http://localhost:3050/api/pipelines \
  -d '{"id":"ghost_dep","depends_on":["ghost"],"source":{"type":"csv","watch_dir":"/tmp/etl_playbook/watched","processed_dir":"/tmp/etl_playbook/processed","poll_interval_secs":15},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"x","unique_key":null}}'
# битый JSON прямо в папку
echo "{ broken" > /tmp/etl_playbook/config/broken.json
sleep 1
```
- [ ] Дубликат id → `409 Pipeline 'via_api' already exists`
- [ ] Без id → `400 config must include an explicit "id" field`
- [ ] Неизвестный `depends_on` → `400 ... unknown pipeline id`, файла `ghost_dep.json` на диске нет (откат сработал ещё до записи)
- [ ] Битый JSON в папке → warning в лог (`New config file ... is invalid, skipping`), процесс жив, `/api/status` не изменился

**Legacy single-file режим не поддерживает динамическую регистрацию:**
```bash
# Ctrl-C текущий сервер, затем:
rm -f /tmp/etl_playbook/legacy_state.json
RUST_LOG=info cargo run -- config/pipeline_csv.json /tmp/etl_playbook/legacy_state.json 3050
curl -s -w "\nHTTP:%{http_code}\n" -X POST http://localhost:3050/api/pipelines \
  -d '{"id":"should_fail","source":{"type":"csv","watch_dir":"/tmp/x","processed_dir":"/tmp/y","poll_interval_secs":15},"transforms":[],"destination":{"type":"postgres","connection_string":"postgres://etl:etlpassword@localhost:5434/etldb","table":"x","unique_key":null}}'
```
- [ ] `400 Dynamic pipeline registration requires directory mode ...`

Ctrl-C, очистка: `rm -rf /tmp/etl_playbook`.

---

## 10. Полная регрессия

```bash
cargo build 2>&1 | grep -c '^warning'   # ожидается: 0
cargo test 2>&1 | tail -5               # ожидается: N passed; 0 failed
```

- [ ] Оба пункта чистые

## 11. Очистка

```bash
docker compose stop postgres   # или `down`, если закончили насовсем
rm -rf /tmp/etl_playbook
rm -f /Users/sherzod/RustForFun/etl-engine/etl_state.json
git status --porcelain   # только ожидаемые/известные изменённые файлы, ничего лишнего
```
