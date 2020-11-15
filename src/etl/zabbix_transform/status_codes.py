from typing import NamedTuple, Dict

ExchangeStatus = NamedTuple(
    'ExchangeStatus', [
        ('code', int),
        ('description', str),
        ('alert', int)
    ]
)

exchange_status_codes_for_state = {
    1 << 0: ExchangeStatus(code=1 << 0, description='0b - запущено (открыто)', alert=1),
    1 << 1: ExchangeStatus(code=1 << 1, description='1b - остановлено (закрыто)', alert=1),
    1 << 2: ExchangeStatus(code=1 << 2, description='2b - обратная связь и задание примерно совпадают', alert=1),
    1 << 3: ExchangeStatus(code=1 << 3, description='3b - "ручное" управление со СКАДА', alert=1),
    1 << 4: ExchangeStatus(code=1 << 4, description='4b - резерв', alert=1),
    1 << 5: ExchangeStatus(code=1 << 5, description='5b - резерв', alert=1),
    1 << 6: ExchangeStatus(code=1 << 6, description='6b - резерв', alert=1),
    1 << 7: ExchangeStatus(code=1 << 7, description='7b - режим "Авто"', alert=1),
    1 << 8: ExchangeStatus(code=1 << 8, description='8b - резерв', alert=1),
    1 << 9: ExchangeStatus(code=1 << 9, description='9b - резерв', alert=1),
    1 << 10: ExchangeStatus(code=1 << 10, description='10b - резерв', alert=1),
    1 << 11: ExchangeStatus(code=1 << 11, description='11b - несовпадение задания и обратной связи', alert=1),
    1 << 12: ExchangeStatus(code=1 << 12, description='12b - предупреждение', alert=1),
    1 << 13: ExchangeStatus(code=1 << 13, description='13b - не готово к запуску', alert=1),
    1 << 14: ExchangeStatus(code=1 << 14, description='14b - авария (запуск)', alert=0),
    1 << 15: ExchangeStatus(code=1 << 15, description='15b - авария (ctrl)', alert=0),
}


def get_status_code_with_define_alert(status_codes: Dict[int, ExchangeStatus], code: int = 0) -> int:
    out_code = None
    for _code, value in status_codes.items():
        if value.alert == code:
            if out_code is None:
                out_code = _code
            out_code = out_code | _code
    return out_code
