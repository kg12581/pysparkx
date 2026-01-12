

if __name__ == '__main__':
    str='iiamstuuudent'

    # 去重
    temp_set = set()
    result = []
    for i in str:
        if i is not None or i != temp_set:
            temp_set.add(i)
            result.append(1)
    print(result)