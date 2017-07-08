# Snowplow Snowflake Loader

## Quickstart

Assuming git, **[Vagrant][vagrant-install]** and **[VirtualBox][virtualbox-install]** installed:

```bash
host$ git clone https://github.com/snowplow/snowplow-snowflake-loader.git
host$ cd snowplow-snowflake-loader
host$ vagrant up && vagrant ssh
guest$ cd /vagrant
guest$ sbt test
```


## Copyright and license

PROPRIETARY AND CONFIDENTIAL

Unauthorized copying of this project via any medium is strictly prohibited.

Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[travis]: https://travis-ci.org/snowplow/snowplowsnowflaketransformer
[travis-image]: https://travis-ci.org/snowplow/snowplowsnowflaketransformer.png?branch=master

[release-image]: http://img.shields.io/badge/release-0.1.0-rc1-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplowsnowflaketransformer/releases
