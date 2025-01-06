# fluent-plugin-impala

이 플러그인은 fluentd의 filter 플러그인으로써 Apache Impala, Kudu의 중요 장애 및 에러 로그를 필터링하는 플러그인입니다.

## Requirement

개발시 macOS 또는 Linux를 권장합니다. 윈도 환경에서는 빌드 환경을 구성하는데 많은 어려움이 있습니다.

* Ruby 3.2.6
* Fluent 1.16.6

### macOS

```
# brew install rbenv ruby-build
# eval "$(rbenv init -)"
# rbenv install 3.2.6
# gem install fluentd -v '1.16.6'
# gem install rufus-scheduler
```
### Windows

```
# MSYS2 and MINGW development toolchain 선택
ridk install
ridk enable
```

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
## Gem 수동 설치

다음의 커맨드를 실행하면 관련 gem을 `vendor/cache` 디렉토리에 모두 다운로드합니다.

```
# bundle package
```

추후 오프라인 환경에서 설치를 하는 경우 다음의 커맨드를 이용할 수 있습니다.

```
# gem install --local gem_file.gem
```
