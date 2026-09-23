using Walrus.Infrastructure.Source;

namespace Walrus.UnitTests;

public sealed class SourceMonitorTests
{
    [Theory]
    [InlineData("*", true)]
    [InlineData("FIRST 1 (*)", true)]
    [InlineData("walrus-eu", true)]
    [InlineData("ANY 1 (pg_b, \"walrus-eu\")", true)]
    [InlineData("FIRST 1 (pg_a, pg_b)", false)]
    [InlineData("pg_b", false)]
    [InlineData("", false)]
    public void A_synchronous_standby_setting_that_would_make_commits_wait_for_capture_is_recognised(string setting, bool names) =>
        SourceMonitor.NamesCapture(setting, "walrus-eu").ShouldBe(names);
}
