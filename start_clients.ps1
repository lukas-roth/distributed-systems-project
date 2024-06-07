param (
    [int]$numClients = 1  # Default to 1 client if not specified
)

# Name of the Conda environment
$envName = "ds"

# Path to the client script
$clientScript = ".\client.py"

# Function to activate Conda environment and start a client
function Start-Client {
    param (
        [int]$clientId
    )
    
    # Construct the command to activate Conda environment and start the client
    $command = "conda activate $envName; python $clientScript client$clientId"
    
    # Start the client in a new process
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $command
}

# Loop to start the specified number of clients
for ($i = 1; $i -le $numClients; $i++) {
    Start-Client -clientId $i
}
