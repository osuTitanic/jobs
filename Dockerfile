FROM python:3.11-bullseye

WORKDIR /jobs

# Installing/Updating system dependencies
RUN apt update -y
RUN apt install postgresql git curl -y

# Install rust toolchain
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install python dependencies
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy source code
COPY . .

ENTRYPOINT [ "python3", "main.py" ]