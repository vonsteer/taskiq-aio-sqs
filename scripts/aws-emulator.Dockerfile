FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal
RUN curl -fsSL -o /tmp/aws-emulator.rpm \
        "https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/latest/aws-emulator-$(uname -m).rpm" \
    && rpm -i /tmp/aws-emulator.rpm && rm /tmp/aws-emulator.rpm \
    && mkdir -p /home/emulator && chown 1000 /home/emulator
USER 1000
ENV HOME=/home/emulator
ENTRYPOINT ["aws-emulator", "-b", "0.0.0.0", "-p", "4566", "-a", "off", "-t", "off"]
