위의 프롬프트에 이어서, nsync_plan.md 의 다음 phase 를 구현해 줘.

테스트 환경은 터미널로 쿠버네티스 파드로 source node 는 "kubectl exec -it ubuntu24-0 -- bash" 을 통해 들어가서 테스트 하면 돼. source path 는 /src 로 생각해. 
ubuntu24-0 와 ubuntu24-1 는 물리적으로 다른 워커 노드에 위치할 수 있다고 가정하고 코드 및 통신 구현해야해.
destination node 는 "kubectl exec -it ubuntu24-1 -- bash" 을 통해 들어가고, destination path 는 /dst 로 생각해.
이 워크스페이스의 mpifileutils 컴파일은 "bash /workspace/dhotcold/install" 를 수행하면 돼.
ubuntu24-0 와 ubuntu24-1 의 바이너리가 다른 경우 hang 이 걸린적이 있으니, 두 노드에서 다 install 을 하도록 해. 단, 두 파드가 같은 디렉토리를 공유하니까 install 은 단독으로 따로따로 진행해서 충돌이 없도록 해.
현재 워크스페이스는 "/workspace/dhotcold/mpifileutils" 에 있어.
코드 수정 완료 후 코드 리뷰도 진행하고, 대용량/많은 파일 sync를 고려해서 최적화를 진행하도록 해.
테스트는 가능한 다양한 케이스들에 대해서 진행해.


```shell
mpirun --allow-run-as-root \
  --prefix /usr/local/openmpi-4.1.8 \
  -x PATH \
  -x LD_LIBRARY_PATH \
  --host 10.244.0.45:2,10.244.0.44:2 \
  -np 4 \
  /usr/local/mpifileutils/bin/nsync \
  --role-mode map --role-map 0:src,1:src,2:dst,3:dst \
  /workspace/test/src2 /workspace/test/dst \
  --batch-files 100000
```
