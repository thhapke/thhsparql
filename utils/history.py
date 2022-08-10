from datetime import datetime
import json

class History(list):
    def __init__(self, filename=None, init_list=None, max_index=100):
        super(History, self).__init__()
        self.max_index = max_index
        self.filename = filename
        if self.filename:
            with open(self.filename) as fp:
                d = json.load(fp)
                self._list = list(d.values())
            self.pointer = len(self._list) - 1
        elif init_list:
            self._list = init_list
            self.pointer = len(self._list) - 1
        else:
            self._list = list()
            self.pointer = -1

    def append(self, item):
        if item in self._list:
            self._list.pop(self._list.index(item))
        elif len(self._list) > self.max_index:
            self._list.pop(0)
        self._list.append(item)
        self.pointer = len(self._list) - 1
        self.save()

    def save(self):
        if self.filename:
            d = dict(zip([str(i) for i in range(0,len(self._list))], self._list))
            with open(self.filename, 'w') as fp:
                json.dump(d,fp)

    def back(self):
        if self.pointer < 0:
            return None
        if self.pointer > 0:
            self.pointer -= 1
        return self._list[self.pointer]

    def forward(self):
        if self.pointer < 0:
            return None
        if self.pointer < len(self._list) - 1:
            self.pointer += 1
        return self._list[self.pointer]

    def last(self):
        self.pointer = len(self._list) - 1
        return self._list[self.pointer]

    def pointer_value(self):
        return self._list[self.pointer]

    def pointer_str(self):
        return f"{self.pointer}/{len(self._list) - 1}"

    def __str__(self):
        str_list = [i for i in self._list]
        return '\n'.join(str_list)


if __name__ == '__main__':

    h = History(None, None, 3)
    for i in range(0, 5):
        h.append(i)

    print(f"List: \n{h}\n")
    print(h.back())
    print(h.back())
    print(h.back())

    print(h.forward())
    print(h.forward())
    print(h.forward())
    print(h.forward())