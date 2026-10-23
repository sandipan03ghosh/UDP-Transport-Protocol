FROM gcc:latest
WORKDIR /work
COPY . .
RUN make
CMD ["bash"]
