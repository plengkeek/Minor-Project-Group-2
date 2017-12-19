def rgb(minimum, maximum, value):
    minimum, maximum = float(minimum), float(maximum)
    ratio = 2 * (value-minimum) / (maximum - minimum)
    b = int(max(0, 255*(1 - ratio)))
    r = int(max(0, 255*(ratio - 1)))
    g = 255 - b - r
    return r, g, b

def converter(minimum, maximum, value):
    r,g,b = rgb(minimum, maximum, value)
    return '#%02x%02x%02x' % (r, g, b)

print converter(-1,100,57)