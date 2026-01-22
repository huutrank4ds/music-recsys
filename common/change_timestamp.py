def parse_duration_to_ms(duration_str):
    """
    Chuyển đổi chuỗi thời gian 'MM:SS' hoặc 'H:MM:SS' sang mili-giây (Int).
    Ví dụ: "3:50" -> 230000
    """
    try:
        parts = list(map(int, duration_str.split(':')))
        seconds = 0
        if len(parts) == 2: # MM:SS
            seconds = parts[0] * 60 + parts[1]
        elif len(parts) == 3: # HH:MM:SS
            seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
        
        return seconds * 1000 # Đổi ra ms
    except:
        return 0