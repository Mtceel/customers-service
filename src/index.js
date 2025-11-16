// Customers Service - Microservice for customer management
import express from 'express';
import { Pool } from 'pg';
import Redis from 'ioredis';
import bcrypt from 'bcryptjs';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import rateLimit from 'express-rate-limit';

const app = express();
const PORT = process.env.PORT || 3004;

// Database connection
const db = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:postgres123@postgres.platform-services.svc.cluster.local:5432/saas_platform',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Redis connection
const redis = new Redis(process.env.REDIS_URL || 'redis://redis.platform-services.svc.cluster.local:6379', {
  retryStrategy: (times) => Math.min(times * 50, 2000),
});

// Middleware
app.use(helmet());
app.use(compression());
app.use(cors());
app.use(express.json());

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 200,
  message: 'Too many requests from this IP, please try again later.',
});
app.use('/api/', limiter);

// Health check
app.get('/health', async (req, res) => {
  try {
    await db.query('SELECT 1');
    await redis.ping();
    
    res.json({ 
      status: 'healthy', 
      service: 'customers-service',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      connections: {
        database: 'connected',
        redis: 'connected'
      }
    });
  } catch (error) {
    res.status(503).json({ 
      status: 'unhealthy', 
      service: 'customers-service',
      error: error.message 
    });
  }
});

// Metrics endpoint
app.get('/metrics', async (req, res) => {
  res.json({
    service: 'customers-service',
    memory: process.memoryUsage(),
    uptime: process.uptime(),
    database: {
      totalConnections: db.totalCount,
      idleConnections: db.idleCount,
    },
  });
});

// ==================== CUSTOMERS API ====================

// GET /api/customers - List customers
app.get('/api/customers', async (req, res) => {
  try {
    const { tenant_id, search, limit = 50, offset = 0 } = req.query;
    
    if (!tenant_id) {
      return res.status(400).json({ error: 'tenant_id is required' });
    }

    // Try cache
    const cacheKey = `customers:${tenant_id}:${search || 'all'}:${limit}:${offset}`;
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    let query = 'SELECT id, email, full_name, phone, address, created_at, updated_at FROM customers WHERE tenant_id = $1';
    const params = [tenant_id];

    if (search) {
      query += ' AND (email ILIKE $2 OR full_name ILIKE $2)';
      params.push(`%${search}%`);
    }

    query += ' ORDER BY created_at DESC LIMIT $' + (params.length + 1) + ' OFFSET $' + (params.length + 2);
    params.push(parseInt(limit), parseInt(offset));

    const result = await db.query(query, params);

    const countQuery = 'SELECT COUNT(*) FROM customers WHERE tenant_id = $1' + 
      (search ? ' AND (email ILIKE $2 OR full_name ILIKE $2)' : '');
    const countParams = search ? [tenant_id, `%${search}%`] : [tenant_id];
    const countResult = await db.query(countQuery, countParams);

    const response = {
      customers: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    };

    await redis.setex(cacheKey, 60, JSON.stringify(response));
    res.json(response);
  } catch (error) {
    console.error('Error fetching customers:', error);
    res.status(500).json({ error: 'Failed to fetch customers' });
  }
});

// GET /api/customers/:id - Get single customer
app.get('/api/customers/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { tenant_id } = req.query;
    
    if (!tenant_id) {
      return res.status(400).json({ error: 'tenant_id is required' });
    }

    const cacheKey = `customer:${id}:${tenant_id}`;
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    const result = await db.query(
      'SELECT id, email, full_name, phone, address, loyalty_points, created_at, updated_at FROM customers WHERE id = $1 AND tenant_id = $2',
      [id, tenant_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Customer not found' });
    }

    await redis.setex(cacheKey, 300, JSON.stringify(result.rows[0]));
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error fetching customer:', error);
    res.status(500).json({ error: 'Failed to fetch customer' });
  }
});

// POST /api/customers - Create new customer
app.post('/api/customers', async (req, res) => {
  try {
    const { tenant_id, email, password, full_name, phone, address } = req.body;
    
    if (!tenant_id || !email || !password) {
      return res.status(400).json({ error: 'tenant_id, email, and password are required' });
    }

    // Check if customer exists
    const existing = await db.query(
      'SELECT id FROM customers WHERE tenant_id = $1 AND email = $2',
      [tenant_id, email]
    );

    if (existing.rows.length > 0) {
      return res.status(409).json({ error: 'Customer already exists' });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    const result = await db.query(
      `INSERT INTO customers (tenant_id, email, password_hash, full_name, phone, address, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
       RETURNING id, email, full_name, phone, address, created_at`,
      [tenant_id, email, hashedPassword, full_name, phone, JSON.stringify(address)]
    );

    await redis.del(`customers:${tenant_id}:*`);
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error('Error creating customer:', error);
    res.status(500).json({ error: 'Failed to create customer' });
  }
});

// PUT /api/customers/:id - Update customer
app.put('/api/customers/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { tenant_id, full_name, phone, address, loyalty_points } = req.body;
    
    if (!tenant_id) {
      return res.status(400).json({ error: 'tenant_id is required' });
    }

    const updates = [];
    const params = [];
    let paramCount = 0;

    if (full_name !== undefined) {
      paramCount++;
      updates.push(`full_name = $${paramCount}`);
      params.push(full_name);
    }
    if (phone !== undefined) {
      paramCount++;
      updates.push(`phone = $${paramCount}`);
      params.push(phone);
    }
    if (address !== undefined) {
      paramCount++;
      updates.push(`address = $${paramCount}`);
      params.push(JSON.stringify(address));
    }
    if (loyalty_points !== undefined) {
      paramCount++;
      updates.push(`loyalty_points = $${paramCount}`);
      params.push(loyalty_points);
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    updates.push(`updated_at = CURRENT_TIMESTAMP`);
    params.push(id, tenant_id);

    const result = await db.query(
      `UPDATE customers SET ${updates.join(', ')} WHERE id = $${paramCount + 1} AND tenant_id = $${paramCount + 2}
       RETURNING id, email, full_name, phone, address, loyalty_points, updated_at`,
      params
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Customer not found' });
    }

    await redis.del(`customer:${id}:${tenant_id}`);
    await redis.del(`customers:${tenant_id}:*`);
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error updating customer:', error);
    res.status(500).json({ error: 'Failed to update customer' });
  }
});

// GET /api/customers/stats/:tenant_id - Customer statistics
app.get('/api/customers/stats/:tenant_id', async (req, res) => {
  try {
    const { tenant_id } = req.params;

    const cacheKey = `customers:stats:${tenant_id}`;
    const cached = await redis.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    const result = await db.query(
      `SELECT 
        COUNT(*) as total_customers,
        SUM(loyalty_points) as total_loyalty_points,
        AVG(loyalty_points) as avg_loyalty_points
      FROM customers WHERE tenant_id = $1`,
      [tenant_id]
    );

    await redis.setex(cacheKey, 120, JSON.stringify(result.rows[0]));
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error fetching customer stats:', error);
    res.status(500).json({ error: 'Failed to fetch customer stats' });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`👥 Customers Service running on port ${PORT}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
});

process.on('SIGTERM', async () => {
  console.log('SIGTERM received, closing connections...');
  await db.end();
  await redis.quit();
  process.exit(0);
});
