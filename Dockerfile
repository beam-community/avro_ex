FROM elixir:1.6-alpine

RUN mix local.hex --force
RUN mix local.rebar --force

WORKDIR /app

COPY config/config.exs config/config.exs
COPY mix.exs .
COPY mix.lock .

RUN mix deps.get
RUN mix deps.compile

COPY . .

RUN mix compile
