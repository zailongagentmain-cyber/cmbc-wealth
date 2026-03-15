# TODO - CMBC Wealth Analyzer

## 当前数据 (2026-03-15)

| 数据类型 | 数量 | 说明 |
|----------|------|------|
| 产品 | 3615 只 | ✅ |
| 净值记录 | 68万+ 条 | 🔄 补抓中 |
| 公告 | 2.7万条 | 🔄 补抓中 |
| PDF解析 | 约100条 | 🔄 进行中 |

---

## 待办事项

### P0: 重新获取全部数据 (进行中)
- [ ] 重新同步净值数据（分页）
- [ ] 重新同步公告数据（分页）
- [ ] 批量解析 PDF 并存入数据库

### P1: 数据验证
- [ ] 验证净值/公告比例
- [ ] 验证 PDF 解析结果

### P2: 分析框架
- [ ] 选择示例产品
- [ ] 定义分析维度

---

## 数据验证规则

1. **净值 vs 公告**: 差异应 <10%
2. **PDF 解析**: announcements.download_status=1 必须在 pdf_parsed_data 有记录

---

## 使用方法

```bash
cd ~/.openclaw/workspace/cmbc-wealth

# 同步单个产品
python3 sync_all.py --product FBAE19001A

# 批量处理 PDF
python3 sync_all.py --pdf-only --limit 100

# 验证数据
python3 sync_all.py --validate
```
