#!/usr/bin/env python3
"""Generate mock review records for testing."""

import json
import os
import random
from datetime import datetime, timedelta

# 模板数据：正面、负面、中性评论模板
POSITIVE_TEMPLATES = [
    "这个{product}的{feature}很不错，{additional}。",
    "{product}质量超出预期，{feature}特别满意。",
    "买给{person}的，{reaction}，{feature}都很好。",
    "已经是第{number}次购买了，{product}依然{quality}。",
    "{feature}很棒，{additional}，值得推荐。",
    "物流超快，{product}{feature}都很好。",
    "客服很耐心，{product}质量也没得说。",
    "性价比很高，{feature}对得起这个价格。",
    "使用了一段时间，{product}{quality}。",
    "{brand}的{product}果然名不虚传，{feature}优秀。",
]

NEGATIVE_TEMPLATES = [
    "这个{product}的{feature}太差了，{problem}。",
    "{product}质量有问题，{problem}，很失望。",
    "客服态度{adj}，{problem}，体验很差。",
    "物流{adj}，{product}还有{problem}。",
    "{feature}不好，{problem}，不建议购买。",
    "和描述不符，{product}{problem}。",
    "用了没几天就{problem}，质量堪忧。",
    "{brand}的{product}让人失望，{feature}不行。",
    "售后服务{adj}，{problem}没人管。",
    "价格不低但{feature}一般，{problem}。",
]

NEUTRAL_TEMPLATES = [
    "{product}的{feature}还可以，{comment}。",
    "整体{quality}，{feature}符合预期。",
    "{product}中规中矩，{comment}。",
    "物流正常，{product}{quality}。",
    "价格{adj}，{feature}还行。",
    "{brand}的{product}一般般，{comment}。",
    "没有惊喜也没有失望，{quality}。",
    "{feature}能接受，{comment}。",
    "用了段时间，{product}{quality}。",
    "不好不坏，{comment}。",
]

# 填充词库
PRODUCTS = ["手机", "耳机", "充电器", "保护壳", "平板", "手表", "键盘", "鼠标", "显示器", "音响"]
FEATURES = ["外观", "性能", "续航", "拍照", "音质", "屏幕", "手感", "做工", "包装", "配件"]
ADDITIONAL = ["物流也很快", "客服态度好", "价格很实惠", "包装很精美", "赠送了贴膜", "说明书很详细"]
PERSONS = ["爸妈", "孩子", "朋友", "同事", "自己"]
REACTIONS = ["很喜欢", "很满意", "说好用", "觉得不错"]
NUMBERS = ["二", "三", "四", "五"]
QUALITIES = ["质量很好", "做工精细", "用料扎实", "很耐用", "表现稳定"]
BRANDS = ["华为", "小米", "苹果", "OPPO", "vivo", "三星", "荣耀"]
PROBLEMS = ["经常卡顿", "电池不耐用", "有划痕", "信号不好", "发热严重", "容易死机"]
ADJS = ["很慢", "很差", "不好", "一般", "偏高", "偏低"]
COMMENTS = ["对得起价格", "没什么亮点", "适合日常使用", "性价比一般", "还可以接受"]

def generate_review(index: int) -> dict:
    """Generate a single review record."""
    sentiment = random.choice(["positive", "negative", "neutral"])
    
    if sentiment == "positive":
        template = random.choice(POSITIVE_TEMPLATES)
    elif sentiment == "negative":
        template = random.choice(NEGATIVE_TEMPLATES)
    else:
        template = random.choice(NEUTRAL_TEMPLATES)
    
    text = template.format(
        product=random.choice(PRODUCTS),
        feature=random.choice(FEATURES),
        additional=random.choice(ADDITIONAL),
        person=random.choice(PERSONS),
        reaction=random.choice(REACTIONS),
        number=random.choice(NUMBERS),
        quality=random.choice(QUALITIES),
        brand=random.choice(BRANDS),
        problem=random.choice(PROBLEMS),
        adj=random.choice(ADJS),
        comment=random.choice(COMMENTS),
    )
    
    # 生成时间戳（过去30天内随机）
    base_time = datetime(2026, 3, 6, 10, 0, 0)
    offset = timedelta(minutes=random.randint(0, 30*24*60))
    timestamp = (base_time + offset).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    return {
        "id": f"r-{index:03d}",
        "text": text,
        "timestamp": timestamp
    }

def main():
    output_path = "/home/ubuntu/llm-pipeline/data/reviews.jsonl"

    review_count = int(os.getenv("REVIEW_COUNT", "10000"))
    review_count = max(1, review_count)
    reviews = [generate_review(i) for i in range(1, review_count + 1)]
    
    with open(output_path, "w", encoding="utf-8") as f:
        for review in reviews:
            f.write(json.dumps(review, ensure_ascii=False) + "\n")
    
    # 统计信息
    sentiments = {"positive": 0, "negative": 0, "neutral": 0}
    for r in reviews:
        # 简单情感统计（基于模板类型）
        text = r["text"]
        if any(w in text for w in ["不错", "满意", "好", "棒", "优秀", "推荐"]):
            sentiments["positive"] += 1
        elif any(w in text for w in ["差", "失望", "不好", "问题", "卡顿"]):
            sentiments["negative"] += 1
        else:
            sentiments["neutral"] += 1
    
    print(f"Generated {len(reviews)} reviews to {output_path}")
    print(f"Sentiment distribution: {sentiments}")
    print("\nPreview (first 3):")
    for r in reviews[:3]:
        print(f"  {r['id']}: {r['text'][:50]}...")

if __name__ == "__main__":
    main()
