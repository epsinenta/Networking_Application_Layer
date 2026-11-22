# Application service

Принимает запросы интерфейса, отправляет их в transport и доставляет результаты клиенту через Server-Sent Events. Исходники и собранная версия веб-интерфейса находятся в `frontend/`.

Требуется Go 1.23 или новее. Запустите transport и agent, затем из корня проекта:

```sh
go run ./cmd/application
```

Откройте `http://localhost:8081`. Для пересборки интерфейса установите Node.js и pnpm и выполните `cd frontend && pnpm install --frozen-lockfile && pnpm build`.

| Переменная | По умолчанию |
| --- | --- |
| `PORT` | `8081` |
| `TRANSPORT_URL` | `http://localhost:8082` |
| `STATIC_DIR` | `frontend/dist` |

`cmd` собирает зависимости; `internal/domain` содержит модели; `internal/usecase` управляет запросами и подключениями; `internal/adapter` содержит HTTP API и клиент транспорта; `internal/config` читает окружение. Проверка: `go test ./...`.
