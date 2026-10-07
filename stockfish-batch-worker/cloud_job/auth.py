"""Refreshable host-login credentials; remote workers still use attached ADC."""
from google.auth.credentials import Credentials


class GcloudCredentials(Credentials):
    def __init__(self, tokens):
        super().__init__()
        self._tokens = tokens

    def _update(self, *, force=False):
        token, expiry = self._tokens.get(force=force, rejected_token=self.token)
        self.token = token
        # google-auth expects a naive UTC expiry datetime.
        self.expiry = expiry.replace(tzinfo=None)

    def before_request(self, request, method, url, headers):
        # Consult the shared provider on EVERY request, including repeated reads
        # using one Storage client for 10+ hours. Usually an in-memory lookup.
        self._update()
        self.apply(headers)

    def refresh(self, request):
        # AuthorizedSession invokes this after 401, even before normal expiry.
        self._update(force=True)
