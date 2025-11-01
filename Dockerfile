FROM python:3.10-bullseye

WORKDIR /ge

RUN apt-get update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

COPY great_expectations/ ./great_expectations

EXPOSE 5000

CMD ["python", "app.py"]
