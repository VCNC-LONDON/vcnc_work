import json
import requests
from datetime import datetime
from pytz import timezone


def send_message_to_slack(text_dict, webhook_channel):
    """
    text_dict의 형태
    1) 단순 메세지만 사용할 경우 {'text': "테스트 메세지3 @kyle :docker:"}
    2) 회색 bar(attachment)는 아래처럼
            {
            "attachments": [
                {
                    "color": "#36a064f",
                    "title": "호호",
                    "text": "테스트 메세지 <@kyle> :docker:"
                }
            ]
        }
    """
    response = requests.post(
        webhook_channel,
        data=json.dumps(text_dict),
        headers={"Content-Type": "application/json"},
    )

    if response.status_code != 200:
        raise ValueError(
            f"Request to slack returned an error {response.status_code}, the response is:\n{response.text}"
        )
    return response.status_code


def make_slack_message(text, title, message_type="info"):
    color = "#008000" if message_type == "info" else "#FF0000"
    if message_type == "error":
        text = f"[{str(datetime.now(timezone('Asia/Seoul')))[:19]}] <@carrot> {text}"
    else:
        text = f"[{str(datetime.now(timezone('Asia/Seoul')))[:19]}] {text}"

    message_dict = {"attachments": [{"color": color, "title": title, "text": text}]}
    return message_dict
