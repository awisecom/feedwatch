FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY feedwatch ./feedwatch
RUN pip install --no-cache-dir ".[postgres]" \
    && useradd --system --uid 10001 --no-create-home feedwatch

USER feedwatch
EXPOSE 8080 8765
ENTRYPOINT ["feedwatch"]
CMD ["--help"]
