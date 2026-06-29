FROM kong:3.7-ubuntu

USER root
RUN luarocks install lua-resty-http && \
    luarocks install lua-resty-session 3.10 && \
    luarocks install lua-resty-openidc
USER kong
