//////////////////////////////////////////////////////////////////////
// ADDINS
//////////////////////////////////////////////////////////////////////
#addin nuget:https://www.nuget.org/api/v2?package=Cake.FileHelpers
#addin nuget:https://www.nuget.org/api/v2?package=Cake.SemVer

//////////////////////////////////////////////////////////////////////
// TOOLS
//////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////

var target = Argument<string>("target", "Default");

var version = Argument<string>("build_version", "0");

var environment = Argument<string>("environment", "");

var eightbotNugetApiKey = Argument<string>("nuget_api_key", "");

var buildType = Argument<string>("build_type", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////
var mainSolution = "Tycho.sln";

var eightbotNugetUsername = "eightbot";
var eightbotNugetSourceName = "Eight-Bot";
var eightbotNugetSourceUrl = "https://eightbot.pkgs.visualstudio.com/_packaging/Eight-Bot/nuget/v3/index.json";

///////////////////////////////////////////////////////////////////////////////
// SETUP / TEARDOWN
///////////////////////////////////////////////////////////////////////////////
Setup(context =>
{
    Information("Building Tycho");
    Information("Nuget API Key: "+ eightbotNugetApiKey);
});

Teardown(context =>
{
    // Executed AFTER the last task.
});

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////
Task ("Clean")
.Does (() =>
{
	CleanDirectories ("./Tycho*/bin");
	CleanDirectories ("./Tycho*/obj");

    var nugetPackages = GetFiles("./*.nupkg");
    DeleteFiles(nugetPackages);
});

Task ("RestorePackages")
.Does (() =>
{
    if(NuGetHasSource(source:eightbotNugetSourceUrl)) {
        NuGetRemoveSource(eightbotNugetSourceName, eightbotNugetSourceUrl);
    }

    NuGetAddSource(
        name: eightbotNugetSourceName,
        source: eightbotNugetSourceUrl,
        settings:  new NuGetSourcesSettings
            {
                UserName = eightbotNugetUsername,
                Password = eightbotNugetApiKey,
                IsSensitiveSource = true,
                Verbosity = NuGetVerbosity.Detailed
            });

	NuGetRestore(mainSolution);
});

Task ("BuildCore")
.IsDependentOn("Clean")
.IsDependentOn("RestorePackages")
.Does (() =>
{
    var buildSettings =
        new MSBuildSettings{}
				.WithProperty("Version", version)
				.WithProperty("ReleaseVersion", version)
				.WithProperty("PackageVersion", version)
				.SetMaxCpuCount(1)
				.SetVerbosity(Verbosity.Quiet)
				.SetConfiguration(buildType)
				.SetPlatformTarget(PlatformTarget.MSIL);

	if (IsRunningOnWindows())
	{
        buildSettings =
            buildSettings
				.UseToolVersion(MSBuildToolVersion.VS2019)
				.SetMSBuildPlatform(MSBuildPlatform.x86);
	}

    if(skipLicenseCheck)
    {
        buildSettings =
            buildSettings
				.WithProperty("AdditionalConstants", "SKIP_LICENSE_CHECK");
    }

    MSBuild(mainSolution, buildSettings);
});

Task ("BuildNuGet")
.IsDependentOn("BuildCore")
.Does (() =>
{
    var nugetSpecs = GetFiles("*.nuspec", SearchScope.Resursive);

    foreach(var nugetSpec in nugetSpecs) {

        var processArguments = new ProcessArgumentBuilder{};

        processArguments
            .Append("pack")
			.Append(nugetSpec.FullPath)
            .Append("-Version")
            .Append(version + environment);

        using(var process =
            StartAndReturnProcess(
                "nuget",
                new ProcessSettings{
                    Arguments = processArguments
                }
            ))
        {
            process.WaitForExit();
        }

    }
});

Task("NuGet")
.IsDependentOn("BuildNuGet")
.Does(() =>
{
    var nugetPackages = GetFiles("./*.nupkg");

    foreach(var package in nugetPackages) {

        var processArguments = new ProcessArgumentBuilder{};

        processArguments
            .Append("push")
            .Append("-Source")
            .Append(eightbotNugetSourceName)
            .Append("-ApiKey")
            .Append(eightbotNugetApiKey)
            .Append(package.FullPath);


        using(var process =
            StartAndReturnProcess(
                "nuget",
                new ProcessSettings{
                    Arguments = processArguments
                }
            ))
        {
            process.WaitForExit();
        }

    }
});

Task("Default")
.IsDependentOn("BuildNuGet")
.Does(() =>
{
    Information("Script Complete");
});

RunTarget(target);
