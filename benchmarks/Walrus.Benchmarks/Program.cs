using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Walrus.Benchmarks.PipelineBenchmarks).Assembly).Run(args);
