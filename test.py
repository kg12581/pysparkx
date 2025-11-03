import ollama
import json


def ollama_chat():
    # 直接调用 Ollama API
    response = ollama.chat(
        model='qwen3:30b',
        messages=[{'role': '假如你是一个大数据开发工程师', 'content': '说中文 出10道sql练习题'}]
    )

    # Ollama 的响应已经是字典格式
    print(f"响应类型: {type(response)}")
    print(f"响应内容: {response}")

    return response["message"]["content"]

if __name__ == '__main__':
    print(ollama_chat())
