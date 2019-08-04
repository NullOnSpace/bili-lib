from conf import settings
from core.scrapy import bilibili_dm as download
from core.db import bili_to_mysql_mulpro as merge


def main():
    main_menu = ("Welcome to bili_helper:\n"
                 "1) download given aid\n"
                 "2) download all\n"
                 "3) merge one avid into mysql\n"
                 "4) merge all into mysql\n"
                 "0) exit\n"
                 "Input Your Choice:"
                )
    choice_dict = {
        '1': choice1,
        '2': choice2,
        '3': choice3,
        '4': choice4,
    }
    while True:
        choice = input(main_menu)
        if choice == '0':
            break
        elif choice in choice_dict:
            choice_dict[choice]()
        else:
            continue


def choice1():
    aid = input('input aid:')
    download.download_aid(aid)


def choice2():
    pass


def choice3():
    aid = input('input avid:')
    merge.main(aid)


def choice4():
    merge.main()
