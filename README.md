# 合作项目数据收件箱

本项目维护合作项目数据收件箱的领域约定与完整收件服务，供后端服务、接口和自动化验证统一使用。当前契约覆盖合作院校填报员、数据协调员、报告编制人员，并明确多格式映射、隔离错误、原子发布、来源谱系等关键约束。

## 领域流程

联合报告截止前，各国院校上传格式不同的数据。收件服务按以下链路处理，避免“错误行直接入库”或“整包退回导致合格数据无法发布”：

1. **接收**：先保存文件指纹（sha256）、提交方与字段映射版本，再解析内容；重复指纹整包拒收并留痕。
2. **映射**：按登记的映射版本把提交方列名解释为统一标准字段；原始数据始终留底。
3. **逐行核验**：按映射版本附带的适用规则逐行判定——合格行进入暂存区，问题行携带违规规则进入隔离区，互不牵连。
4. **受控修订**：隔离行可由协调员凭修改人、理由做修订，修订后重新核验，全部修订履历留痕。
5. **原子发布**：仅核验通过的行一次性下沉正式数据；发布过程串行化（互斥锁 + 乐观版本号），下沉失败整体回滚，不留半成品。
6. **撤回**：未发布批次归档，已发布批次连带从正式数据移除。
7. **来源谱系**：不可变事件台账记录接收、隔离、修订、换版、发布（含失败）、撤回、重复拒收；每行可查当前状态与去向（`row_trace`）。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/data_inbox/`：收件服务实现（映射、核验、隔离、修订、发布、撤回、谱系）。
- `tools/check_contract.py`：命令行契约摘要检查。
- `tools/inbox_demo.py`：收件服务端到端演示。
- `tests/`：契约回归测试与收件服务全链路测试（含并发发布与失败回滚）。

## 验证

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`

端到端演示：`python3 tools/inbox_demo.py`

## 快速示例

```python
from data_inbox import InboxService, MappingRegistry, FieldMapping, required, numeric

registry = MappingRegistry()
registry.register(FieldMapping(
    version="map-cn-v1", submitter="中国合作院校",
    columns={"院校代码": "school_code", "学生数": "student_count"},
    rules=(required("R1", "school_code"), numeric("R2", "student_count")),
))
service = InboxService(registry)
batch = service.receive(
    filename="batch.csv", content=content_bytes,
    submitter="华东合作大学", mapping_version="map-cn-v1",
)
print(batch.counts())            # 合格/隔离/已发布/已撤回各行数
service.publish(batch.batch_id, by="报告编制人员")   # 仅合格行原子下沉
print(service.row_trace(row_id)) # 任一行动态、去向、修订与谱系
```
