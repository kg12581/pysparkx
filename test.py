# # import ollama
# # import json
# #
# #
# # def ollama_chat():
# #     # 直接调用 Ollama API
# #     response = ollama.chat(
# #         model='qwen3:30b',
# #         messages=[{'role': '假如你是一个大数据开发工程师', 'content': '说中文 出10道sql练习题'}]
# #     )
# #
# #     # Ollama 的响应已经是字典格式
# #     print(f"响应类型: {type(response)}")
# #     print(f"响应内容: {response}")
# #
# #     return response["message"]["content"]
# #
# # if __name__ == '__main__':
# #     print(ollama_chat())
#
#
#
# def remove_duplicates_optimized(s):
#     seen = set()  # 判重O(1)，空间换时间
#     result = []
#     for char in s:
#         if char not in seen:
#             seen.add(char)
#             result.append(char)
#     return "".join(result)
#
#
# print(remove_duplicates_optimized('abbbbcdddefddggg'))


# import easyocr
#
# reader = easyocr.Reader(['en','ch_sim'])  # 初始化（第一次会下载模型）
# result = reader.readtext('img.png', detail=0)  # detail=0 返回文本列表
# print("\n".join(result))


import easyocr
import cv2


def recognize_captcha_easyocr(image_path):
    # 读取图片
    img = cv2.imread(image_path)
    if img is None:
        return "无法读取图片"

    # 预处理：转为灰度图（减少干扰）
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 初始化 EasyOCR 阅读器（仅识别英文和数字）
    reader = easyocr.Reader(['en'], gpu=False)  # 若有GPU可设为True

    # 识别验证码
    result = reader.readtext(gray, detail=0)  # detail=0 只返回识别结果

    # 拼接结果（验证码通常是连续字符）
    return ''.join(result) if result else "识别失败"

import ssl  # 新增：禁用 SSL 验证



# 测试
if __name__ == "__main__":
    # 临时禁用 SSL 证书验证（仅测试用）
    ssl._create_default_https_context = ssl._create_unverified_context

    captcha_image = "img.png"  # 替换为你的验证码图片路径
    result = recognize_captcha_easyocr(captcha_image)
    print(f"识别结果：{result}")