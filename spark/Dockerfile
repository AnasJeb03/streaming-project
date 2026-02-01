FROM apache/spark:3.5.0-python3

USER root

# Install required packages
RUN pip install --no-cache-dir pyspark==3.5.0

# Create app directory
WORKDIR /app

# Copy spark scripts
COPY . .

# Keep container running
CMD ["tail", "-f", "/dev/null"]