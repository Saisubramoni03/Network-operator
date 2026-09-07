function ApiUnavailableBanner({ message }) {
  return (
    <div className="api-unavailable-banner">
      <strong>API unavailable:</strong> {message}
    </div>
  );
}

export default ApiUnavailableBanner;