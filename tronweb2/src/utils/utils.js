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
