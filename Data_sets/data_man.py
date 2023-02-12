def format_txt(string):
    s = string.split(",")
    return f"{s[0]} {s[1]}  {s[2]},{s[3]}"


with open("orders_new.txt",'r') as read_file:
    with open("new_order.txt",'w') as write_file:
        for i in read_file:
            write_file.writelines(format_txt(i))
