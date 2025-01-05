# fluent-plugin-impala

[Fluentd](https://fluentd.org/) filter plugin to do something.

TODO: write description for you plugin.

## Requirement

* Ruby 3.2.6 (`brew install rbenv ruby-build; rbenv install 3.2.6`)
* Fluent 1.16.6 (`gem install fluentd -v '1.16.6'`)

## Installation

### RubyGems

```
$ gem install fluent-plugin-impala
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-impala"
```

And then execute:

```
$ bundle
```

## Configuration

You can generate configuration template:

```
$ fluent-plugin-config-format filter impala
```

You can copy and paste generated documents here.

## Copyright

* Copyright(c) 2025- Edward KIM
* License
  * Apache License, Version 2.0
