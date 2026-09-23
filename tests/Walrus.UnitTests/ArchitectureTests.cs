using System.Reflection;
using Walrus.Application.Capture;
using Walrus.Domain;
using Walrus.Infrastructure;

namespace Walrus.UnitTests;

/// <summary>
/// The layering, checked against what each compiled assembly actually references rather than against the
/// project files. A project reference nothing uses does not show up here, and a type used through a transitive
/// reference does, which is the dependency that matters.
/// </summary>
public sealed class ArchitectureTests
{
    private static readonly Assembly Domain = typeof(ChangeEvent).Assembly;
    private static readonly Assembly Application = typeof(CaptureSession).Assembly;
    private static readonly Assembly Infrastructure = typeof(WalrusOptions).Assembly;
    private static readonly Assembly Api = Assembly.Load("Walrus.Api");

    // Concrete technologies no inner layer may name. Logging abstractions are the one exception, allowed in
    // Application on purpose: the capture and dispatch loops are use cases, and they have to say why they failed.
    private static readonly string[] Technologies =
    [
        "Npgsql",
        "Microsoft.EntityFrameworkCore",
        "Microsoft.AspNetCore",
        "Microsoft.Extensions.Hosting",
        "Microsoft.Extensions.Http",
        "Microsoft.Extensions.Options",
        "Microsoft.Extensions.DependencyInjection",
        "OpenTelemetry",
    ];

    private static string[] References(Assembly assembly) =>
        [.. assembly.GetReferencedAssemblies().Select(static name => name.Name!)];

    private static string[] Projects(Assembly assembly) =>
        [.. References(assembly).Where(static name => name.StartsWith("Walrus.", StringComparison.Ordinal))];

    [Fact]
    public void The_domain_depends_on_nothing_but_the_base_library() =>
        References(Domain).ShouldAllBe(name => name.StartsWith("System", StringComparison.Ordinal));

    [Fact]
    public void The_application_depends_only_on_the_domain_and_logging_abstractions()
    {
        Projects(Application).ShouldBe(["Walrus.Domain"]);
        References(Application)
            .Where(static name => !name.StartsWith("System", StringComparison.Ordinal) && !name.StartsWith("Walrus.", StringComparison.Ordinal))
            .ShouldBe(["Microsoft.Extensions.Logging.Abstractions"]);
    }

    [Fact]
    public void Neither_inner_layer_names_a_concrete_technology()
    {
        foreach (Assembly inner in (Assembly[])[Domain, Application])
        {
            References(inner).ShouldNotContain(
                name => Technologies.Any(technology => name.StartsWith(technology, StringComparison.Ordinal)),
                $"{inner.GetName().Name} references a technology");
        }
    }

    [Fact]
    public void Infrastructure_implements_the_application_and_knows_nothing_of_the_host()
    {
        Projects(Infrastructure).ShouldContain("Walrus.Application");
        Projects(Infrastructure).ShouldNotContain("Walrus.Api");
    }

    [Fact]
    public void The_host_reaches_the_pipeline_through_infrastructure() =>
        Projects(Api).ShouldContain("Walrus.Infrastructure");
}
