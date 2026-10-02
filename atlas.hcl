env "local" {
  url = getenv("DATABASE_URL")
  migration {
    dir = "file://packages/db/migrations"
  }
}
