param (
    [int]$num = 1  # Default to 1 client if not specified
)

# Name of the Conda environment
$envName = "ds"

# Path to the client script
$clientScript = ".\server.py"  # Use the full path to the client script

# Function to start a client with resizing
function Start-Client {
    
    
    # Start the client in a new process with resizing
    $command = "powershell -NoExit -File .\resize.ps1 -width 60 -height 13 -envName `"$envName`" -clientScript `"$clientScript`""
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $command
}

# Loop to start the specified number of clients
for ($i = 1; $i -le $num; $i++) {
    Start-Client
}
