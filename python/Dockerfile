FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mensagem_pb2.py servidor.py bot.py ./
CMD ["python", "servidor.py"]