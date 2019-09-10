from hashlib import sha1


def get_id(self, ip, port):
    hash = sha1()
    hash.update(ip.encode())
    hash.update(port.to_bytes(2, "big"))
    return hash.digest()
