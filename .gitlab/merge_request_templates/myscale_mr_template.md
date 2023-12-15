<!--

MR 的标题应该符合 commit message 的标题规范:
http://mqdb.page.moqi.ai/myscale-internal-docs/development/get-started/commit-and-submit-a-merge-request/#commit-message

-->

### 背景
<!--

请先创建一个 issue 描述你要解决的问题。

这里必须有一行以 "Issue Number:  " 开头，并且通过
"close" 或 "ref" 引用相关的 issue。

详情见 http://mqdb.page.moqi.ai/myscale-internal-docs/development/how-to-work/workflow/

-->

问题描述：...

Issue Number: close #xxx, ref #xxx

### 描述你的修改和工作原理

### 需重点关注列表

### 检查列表

请负责人检查自己是否

- [ ] 完成了必要的测试 <!-- 至少应该完成一项 -->
    - [ ] Unit test
    - [ ] Integration test
    - [ ] Manual test (add detailed scripts or steps below)
    - [ ] No code is changed
- [ ] 记录了 MR 的重要改动或副作用
    - [ ] Performance regression: Consumes more CPU
    - [ ] Performance regression: Consumes more Memory
    - [ ] Breaking backward compatibility
        - [ ] 是否需要修改现有系统的数据文件，索引文件的文件格式或者文件后缀
        - [ ] 是否需要对现有系统的数据文件或索引文件进行迁移
- [ ] 更新了相关的用户手册
    - [ ] Affects user behaviors
    - [ ] Contains syntax changes
    - [ ] Contains variable changes
    - [ ] Contains experimental features

### 发行说明

<!-- compatibility change, improvement, bugfix, and new feature need a release note -->

参考
[发行说明语言风格指南](http://mqdb.page.moqi.ai/myscale-internal-docs/development/how-to-work/release-notes-style-guide/)
写一个高质量的发行说明。

```release-note
None
```

