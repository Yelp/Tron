export function fetchFromApi(endpoint, dataCallback) {
    // Can change for testing
    var apiPrefix = '';
    fetch(apiPrefix + endpoint)
        .then(response => response.json())
        .then(data => dataCallback(data));
}

export function getJobColor(status) {
    switch (status) {
        case "running":
            return "primary";
        case "disabled":
            return "warning";
        case "enabled":
            return "success";
        default:
            return "light";
    }
}
