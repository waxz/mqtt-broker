---
title: Mqtt Broker
emoji: 🦀
colorFrom: indigo
colorTo: blue
sdk: docker
pinned: false
license: mit
short_description: mqtt-broker
---

Check out the configuration reference at https://huggingface.co/docs/hub/spaces-config-reference

## Dependency
- iotcore:https://github.com/waxz/iotcore

## BUild
```bash

python pyx/setup.py build_ext -b ./

```
## Run
```bash
python run_broker.py  > /dev/null 2>&1
```


### Reference
- https://discuss.huggingface.co/t/hf-space-stuck-at-starting/170911/2
- https://stackoverflow.com/questions/35244333/in-general-does-redirecting-outputs-to-dev-null-enhance-performance
- https://askubuntu.com/questions/350208/what-does-2-dev-null-mean
