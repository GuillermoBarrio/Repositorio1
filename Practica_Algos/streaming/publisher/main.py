
import os  # for importing env vars for the bot to use
import sys
import json
import time

from twitchio.ext import commands
from google.cloud import pubsub_v1
from loguru import logger

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_NAME = os.getenv("TOPIC_NAME")

TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"

publisher = pubsub_v1.PublisherClient()


class Bot(commands.Bot):

    def __init__(self, irc_token='...', client_id='...', nick='...', prefix="!", initial_channels=['...'], debug=True):
        super().__init__(irc_token=irc_token, client_id=client_id, nick=nick, prefix='!',
                         initial_channels=initial_channels)
        self.debug = debug

    # Events don't need decorators when subclassed
    async def event_ready(self):
        logger.info('Ready')

    async def event_message(self, message):
        logger.info(message.content)
        publisher.publish(TOPIC_PATH, str.encode(message.content))


def main(request):

    topic_name = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
    # publisher.create_topic(topic_name)

    request_json = request.get_json(silent=True)

    logger.info("Starting listener...")
    if "debug" in request_json and isinstance(request_json["debug"], bool):
        logger.info(f"Debug mode: {request_json['debug']}")
        bot = Bot(
          # set up the bot
          irc_token="oauth:xl5cpf8qe8tl1d03dppymchi6r04iz",
          client_id="ciliqxi534iwg4pfqj7swl1jmkt23y",
          nick="franalgaba",
          prefix="!",
          initial_channels=["franalgaba"],
          debug=request_json['debug'])
    else:
        bot = Bot(
          # set up the bot
          irc_token="oauth:xl5cpf8qe8tl1d03dppymchi6r04iz",
          client_id="ciliqxi534iwg4pfqj7swl1jmkt23y",
          nick="franalgaba",
          prefix="!",
          initial_channels=["franalgaba"])

    bot.run()