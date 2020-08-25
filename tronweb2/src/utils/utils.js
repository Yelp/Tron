export function fetchFromApi(endpoint, dataCallback) {
  // Can change for testing
  const apiPrefix = '';
  const url = apiPrefix + endpoint;
  fetch(url)
    .then((response) => {
      if (!response.ok) {
        return { error: { message: response.statusText, code: response.status } };
      }
      return response.json();
    })
    .then((data) => dataCallback(data))
    .catch((error) => {
      console.error(`Error fetching ${url}`, error);
      dataCallback({ error: { message: 'connection error' } });
    });
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
