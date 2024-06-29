param (
    [int]$numClients = 1  # Default to 1 client if not specified
)

# Name of the Conda environment
$envName = "ds"

# Path to the client script
$clientScript = ".\client.py"  # Use the full path to the client script

# Function to start a client with resizing
function Start-Client {
    param (
        [int]$clientId
    )

    # Set environment variables for Unicode support
    [System.Environment]::SetEnvironmentVariable("PYTHONIOENCODING", "utf-8", "Process")
    [System.Environment]::SetEnvironmentVariable("PYTHONUTF8", 1, "Process")
    
    # Start the client in a new process with resizing
    $command = "powershell -NoExit -File .\resize.ps1 -width 80 -height 13 -envName `"$envName`" -clientScript `"$clientScript`" -clientId $clientId"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $command
}

# Loop to start the specified number of clients
for ($i = 1; $i -le $numClients; $i++) {
    Start-Client -clientId $i
}
