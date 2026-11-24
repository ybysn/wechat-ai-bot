FROM python:3.14-slim-bookworm

ENV PYTHONPATH=/opt/wechat-ai-bot/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1

WORKDIR /opt/wechat-ai-bot
COPY src/wechat_ai_bot/ ./src/wechat_ai_bot/

USER 65534:65534
CMD ["python", "-m", "wechat_ai_bot.consumer", "--config", "/config/config.json"]
