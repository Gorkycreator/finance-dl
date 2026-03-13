import pytest
import base64
import requests

from finance_dl.simplefin_access_token_setup import exchange_setup_token


def test_raise_on_bad_base64_input():
    with pytest.raises(ValueError, match="Token decoding failed"):
        exchange_setup_token('abcdefg')


def test_successful_run(mocker):
    url = "https://fake-url.com"
    success_url = "https://another-fake-url.com"
    token = base64.b64encode(url.encode('utf-8')).decode('utf-8')

    mock_post = mocker.patch("requests.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.text = success_url + "   "


    result = exchange_setup_token(token)

    mock_post.assert_called_once_with(url)

    assert result == success_url


def test_403_error(mocker):
    url = "https://fake-url.com"
    token = base64.b64encode(url.encode('utf-8')).decode('utf-8')
    
    # Mock a response that raises an HTTPError on .raise_for_status()
    mock_response = mocker.Mock()
    mock_response.status_code = 403
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    
    mocker.patch("requests.post", return_value=mock_response)
    
    with pytest.raises(requests.exceptions.HTTPError):
        exchange_setup_token(token)