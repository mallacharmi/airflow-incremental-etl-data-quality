INSERT INTO
    public.products
VALUES ('P1', 'Laptop'),
    ('P2', 'Mobile'),
    ('P3', 'Headphones'),
    ('P4', 'Keyboard'),
    ('P5', 'Mouse'),
    ('P6', 'Monitor'),
    ('P7', 'Tablet'),
    ('P8', 'Camera'),
    ('P9', 'Printer'),
    ('P10', 'Smart Watch') ON CONFLICT (product_id) DO NOTHING;