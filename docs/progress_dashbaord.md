# PATHWAY PROGRESS DASHBOARD

<table>
<thead>
  <tr>
    <th>connector</th>
    <th>no. messages in<br>the last minibatch</th>
    <th>in the last<br>minute</th>
    <th>since start</th>
    <th>operator</th>
    <th>latency to wall clock [ms]</th>
    <th>lag to input [ms]</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>PythonReader-0</td>
    <td>96</td>
    <td>969</td>
    <td>969</td>
    <td>input</td>
    <td>30</td>
    <td></td>
  </tr>
  <tr>
    <td>FilesystemReader-1</td>
    <td>71</td>
    <td>928</td>
    <td>928</td>
    <td>output</td>
    <td>30</td>
    <td>0</td>
  </tr>
</tbody>
</table>

Above you can see the latency of input and output operators.
The latency is measured as the difference between the time when the operator processed the data and the time when pathway acquired the data.

---

## LOGS

```
[07/18/23 11:24:06] INFO     Preparing Pathway computation
[07/18/23 11:24:07] INFO     PythonReader-0: 92 entries (1 minibatch(es)) have been sent to the engine
[07/18/23 11:24:07] INFO     FilesystemReader-1: 161 entries (1 minibatch(es)) have been sent to the engine
[07/18/23 11:24:13] INFO     PythonReader-0: 577 entries (6 minibatch(es)) have been sent to the engine
[07/18/23 11:24:13] INFO     FilesystemReader-1: 300 entries (6 minibatch(es)) have been sent to the engine
```

## pathway dashboard -> rpcstream
| Pathway concept | Your equivalent                                |
| --------------- | ---------------------------------------------- |
| connector       | fetcher / kafka sink                           |
| operator        | fetch / process / sink                         |
| messages        | blocks / tx rows                               |
| minibatch       | batch of blocks                                |
| latency         | RPC latency / end-to-end latency               |
| lag             | (realtime mode) latest_block - processed_block |
