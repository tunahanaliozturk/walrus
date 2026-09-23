# Contributing

## Before you start

You need the .NET 10 SDK, Node 24 and Docker. Everything else runs in containers.

```bash
dotnet build Walrus.slnx
dotnet run --project tests/Walrus.UnitTests
dotnet run --project tests/Walrus.IntegrationTests      # Postgres 18 in a container
cd web && npm ci && npm test
```

## What a change needs

- **Warnings are errors.** The build runs the analyzers at `latest-recommended` with code style enforced. Fix the
  warning; suppress only with a justification the next reader can argue with.
- **Formatting is checked.** Run `dotnet format whitespace Walrus.slnx` and `dotnet format style Walrus.slnx`, and
  `npm run format` in `web`.
- **A test that could fail.** A change to capture or dispatch comes with a test against a real Postgres, or a property
  test if it is about ordering or convergence. A test that passes whether or not the change is there proves nothing.
- **Layers point inward.** Domain depends on nothing, Application on Domain and logging abstractions, Infrastructure
  on Application, the API on Infrastructure. `ArchitectureTests` reads the compiled assemblies and fails otherwise.
- **The store is an EF Core context, used directly.** No repository layer over it. A migration for every model change:
  `dotnet ef migrations add <Name> --project src/Walrus.Infrastructure --output-dir Store/Migrations`.
- **A changed API regenerates the console's types.** `dotnet build src/Walrus.Api -p:OpenApiGenerateDocuments=true`,
  then `npm run contract` in `web`. CI fails if either is out of date.
- **Dependencies are permissively licensed**, at every depth. `dotnet run --project tools/Walrus.LicenseAudit -- .`
  and `npm run licences` check.
- **A claim about speed has a number behind it**, measured with `load/Walrus.Load` or BenchmarkDotNet, with the
  hardware named.

## Writing

Documentation and comments are in English and explain why, not what. Short sentences, real numbers, and the
limitations stated plainly.

## Commits

Conventional commit prefixes (`feat`, `fix`, `test`, `docs`, `build`, `ci`, `refactor`), one logical change per
commit, and a body that says why when the subject cannot.
