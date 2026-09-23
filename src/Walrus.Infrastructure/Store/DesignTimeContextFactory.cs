using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Walrus.Infrastructure.Store;

/// <summary>
/// Lets <c>dotnet ef migrations add</c> build the model without starting the service. It never connects: the
/// connection string only has to parse.
/// </summary>
internal sealed class DesignTimeContextFactory : IDesignTimeDbContextFactory<WalrusDbContext>
{
    /// <inheritdoc />
    public WalrusDbContext CreateDbContext(string[] args) =>
        new(new DbContextOptionsBuilder<WalrusDbContext>()
            .UseNpgsql("Host=localhost;Database=walrus")
            .UseSnakeCaseNamingConvention()
            .Options);
}
