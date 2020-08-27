export function fetchFromApi(endpoint, dataCallback) {
  // Can change for testing
  const apiPrefix = '';
  const url = apiPrefix + endpoint;

  // Return function to skip the callback
  let cancelled = false;
  function cancel() {
    cancelled = true;
  }

  fetch(url)
    .then((response) => {
      if (!response.ok) {
        return { error: { message: response.statusText, code: response.status } };
      }
      return response.json();
    })
    .then((data) => {
      if (!cancelled) {
        dataCallback(data);
      }
    })
    .catch((error) => {
      console.error(`Error fetching ${url}`, error);
      if (!cancelled) {
        dataCallback({ error: { message: 'connection error' } });
      }
    });

  return cancel;
}

export function getJobColor(status) {
  switch (status) {
    case 'running':
      return 'primary';
    case 'disabled':
      return 'warning';
    case 'enabled':
      return 'success';
    default:
      return 'light';
  }
}
