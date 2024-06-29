param (
    [int]$width = 120,
    [int]$height = 40,
    [string]$envName,
    [string]$clientScript,
    [int]$clientId
)

# Set the window size
$pswindow = $host.UI.RawUI
$pswindow.BufferSize = New-Object Management.Automation.Host.Size ($width, $pswindow.BufferSize.Height)
$pswindow.WindowSize = New-Object Management.Automation.Host.Size ($width, $height)

# Activate Conda environment and start the client script
$command = "conda activate $envName; python `"$clientScript`" client$clientId"
Invoke-Expression $command
