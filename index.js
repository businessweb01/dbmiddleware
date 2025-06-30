import { rtdb, ref, onChildChanged, onValue, remove } from './firebaseconfig.js';
import express from 'express';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

// Robust tracking and state management
let processedBookings = new Set();
let totalProcessed = 0;
let lastActivity = new Date();
let connectionStatus = 'connecting';
let errorCount = 0;
let retryCount = 0;
let isShuttingDown = false;

// Worker identification for Railway
const WORKER_ID = `worker_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
const START_TIME = new Date().toISOString();

// Environment config
const LARAVEL_API_URL = process.env.LARAVEL_API_URL || 'https://api.multinetworkcatv.com/api/insert-booking';
const isDev = process.env.NODE_ENV !== 'production';

console.log(`🚀 Starting Robust Firebase Worker`);
console.log(`🆔 Worker ID: ${WORKER_ID}`);
console.log(`📡 API: ${LARAVEL_API_URL}`);
console.log(`🔧 Mode: ${isDev ? 'DEV' : 'PROD'}`);
console.log(`⏰ Started: ${START_TIME}`);

// Robust health endpoints
app.get('/', (req, res) => {
  const uptime = Math.floor(process.uptime());
  res.json({ 
    status: connectionStatus,
    worker_id: WORKER_ID,
    processed: totalProcessed,
    uptime_seconds: uptime,
    uptime_display: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m`,
    last_activity: lastActivity.toISOString(),
    errors: errorCount,
    retries: retryCount,
    cache_size: processedBookings.size,
    memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
    started_at: START_TIME,
    timestamp: new Date().toISOString()
  });
});

app.get('/health', (req, res) => {
  const isHealthy = connectionStatus === 'connected' && !isShuttingDown;
  res.status(isHealthy ? 200 : 503).json({
    healthy: isHealthy,
    status: connectionStatus,
    uptime: Math.floor(process.uptime()),
    processed: totalProcessed
  });
});

// Keep-alive endpoint for external monitoring
app.get('/ping', (req, res) => {
  lastActivity = new Date();
  res.json({ 
    pong: true, 
    timestamp: lastActivity.toISOString(),
    worker_id: WORKER_ID
  });
});

// Force garbage collection endpoint (for memory management)
app.post('/gc', (req, res) => {
  if (global.gc) {
    const before = process.memoryUsage().heapUsed;
    global.gc();
    const after = process.memoryUsage().heapUsed;
    res.json({ 
      gc_performed: true,
      memory_freed_mb: Math.round((before - after) / 1024 / 1024)
    });
  } else {
    res.json({ gc_performed: false, message: 'GC not available' });
  }
});

const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🌐 Robust worker HTTP server running on port ${PORT}`);
  console.log(`📊 Health: http://localhost:${PORT}/health`);
  console.log(`🏓 Ping: http://localhost:${PORT}/ping`);
});

// Robust booking processing with retry logic
async function processBookingWithRetry(booking, firebaseKey, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await processBooking(booking, firebaseKey);
      return; // Success, exit retry loop
    } catch (error) {
      console.error(`❌ Attempt ${attempt}/${maxRetries} failed for ${booking.bookingId}: ${error.message}`);
      
      if (attempt === maxRetries) {
        console.error(`💥 Final attempt failed for ${booking.bookingId}, giving up`);
        processedBookings.delete(booking.bookingId); // Allow future retry
        errorCount++;
        return;
      }
      
      // Exponential backoff: 2s, 4s, 8s
      const delay = Math.pow(2, attempt) * 1000;
      console.log(`⏳ Retrying ${booking.bookingId} in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
      retryCount++;
    }
  }
}

// Enhanced booking processing
async function processBooking(booking, firebaseKey) {
  if (isShuttingDown) {
    throw new Error('Worker is shutting down');
  }

  processedBookings.add(booking.bookingId);
  lastActivity = new Date();
  
  console.log(`📤 Processing: ${booking.bookingId} (${booking.booking_status})`);
  
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);
  
  try {
    const response = await fetch(LARAVEL_API_URL, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'User-Agent': `Railway-Worker/${WORKER_ID}`,
        'X-Worker-ID': WORKER_ID
      },
      body: JSON.stringify({
        assignedRider: booking.assignedRider || null,
        auto_cancel_at: booking.auto_cancel_at || null,
        booked_at: booking.booked_at || null,
        booked_at_timestamp: booking.booked_at_timestamp || null,
        bookingId: booking.bookingId,
        booking_status: booking.booking_status,
        plate_number: booking.plate_number || null,
        destinationCoordinates: booking.destinationCoordinates || null,
        expires_at: booking.expires_at || null,
        fare: booking.fare || 0,
        luggageCount: booking.luggageCount || 0,
        numberofPassengers: booking.numberofPassengers || "1",
        passengerId: booking.passengerId || null,
        passengerName: booking.passengerName || null,
        paymentMethod: booking.paymentMethod || "Cash",
        pickupCoordinates: booking.pickupCoordinates || null,
        ratings: booking.ratings || null,
        // Metadata
        processed_at: new Date().toISOString(),
        worker_id: WORKER_ID
      }),
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const errorText = await response.text().catch(() => 'Unknown error');
      throw new Error(`HTTP ${response.status}: ${errorText}`);
    }

    console.log(`✅ Sent: ${booking.bookingId}`);
    totalProcessed++;
    
    // Delete from Firebase (only in production)
    if (!isDev) {
      await remove(ref(rtdb, `Bookings/${firebaseKey}`));
      console.log(`🗑️ Deleted: ${booking.bookingId}`);
    }
    
  } catch (error) {
    clearTimeout(timeoutId);
    
    if (error.name === 'AbortError') {
      throw new Error('Request timeout');
    }
    
    throw error;
  }
}

// Check if booking should be processed
function shouldProcess(booking) {
  if (!booking?.bookingId) return false;
  if (processedBookings.has(booking.bookingId)) return false;
  
  const targetStatuses = ['Cancelled', 'Complete', 'Completed'];
  return targetStatuses.includes(booking.booking_status);
}

// Robust Firebase connection with auto-reconnection
async function initializeFirebaseConnection() {
  try {
    console.log('🔥 Initializing Firebase connection...');
    connectionStatus = 'connecting';
    
    const bookingsRef = ref(rtdb, 'Bookings');
    
    console.log('👀 Setting up Firebase listeners...');
    
    // Listen for booking changes
    onChildChanged(bookingsRef, async (snapshot) => {
      try {
        const booking = snapshot.val();
        const key = snapshot.key;
        
        if (!booking) return;
        
        console.log(`🔄 Change detected: ${key} - ${booking?.booking_status}`);
        lastActivity = new Date();
        
        if (shouldProcess(booking)) {
          console.log(`🚀 Queuing: ${booking.bookingId}`);
          await processBookingWithRetry(booking, key);
        }
      } catch (error) {
        console.error(`❌ Error in change listener: ${error.message}`);
        errorCount++;
      }
    }, (error) => {
      console.error(`🔥 Firebase onChildChanged error:`, error);
      connectionStatus = 'error';
      scheduleReconnection();
    });

    // Initial scan
    console.log('📊 Performing initial scan...');
    onValue(bookingsRef, async (snapshot) => {
      try {
        const data = snapshot.val();
        if (!data) {
          console.log(`📊 No existing bookings found`);
          connectionStatus = 'connected';
          return;
        }
        
        const bookings = Object.entries(data);
        console.log(`📊 Scanning ${bookings.length} existing bookings`);
        lastActivity = new Date();
        
        let processedCount = 0;
        for (const [key, booking] of bookings) {
          if (shouldProcess(booking)) {
            await processBookingWithRetry(booking, key);
            processedCount++;
          }
        }
        
        console.log(`✨ Initial scan complete: ${processedCount} bookings processed`);
        connectionStatus = 'connected';
        
      } catch (error) {
        console.error(`❌ Error in initial scan: ${error.message}`);
        connectionStatus = 'error';
        errorCount++;
      }
    }, { onlyOnce: true });
    
  } catch (error) {
    console.error(`🔥 Firebase initialization failed:`, error);
    connectionStatus = 'error';
    scheduleReconnection();
  }
}

// Auto-reconnection logic
function scheduleReconnection() {
  if (isShuttingDown) return;
  
  const delay = Math.min(30000, 5000 * Math.pow(2, Math.min(retryCount, 4))); // Max 30s
  console.log(`🔄 Scheduling reconnection in ${delay}ms...`);
  
  setTimeout(() => {
    if (!isShuttingDown) {
      retryCount++;
      initializeFirebaseConnection();
    }
  }, delay);
}

// Self-monitoring and keep-alive system
function startRobustMonitoring() {
  // Status logging every 3 minutes
  setInterval(() => {
    if (isShuttingDown) return;
    
    const uptime = Math.floor(process.uptime() / 60);
    const memMB = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    
    console.log(`💓 Worker Status: ${uptime}m uptime | ${totalProcessed} processed | ` +
               `${errorCount} errors | ${retryCount} retries | ${memMB}MB | ${connectionStatus}`);
    
    lastActivity = new Date();
  }, 3 * 60 * 1000);

  // Memory management every 20 minutes
  setInterval(() => {
    if (isShuttingDown) return;
    
    const memUsage = process.memoryUsage();
    const memMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    
    // Clean cache if too large
    if (processedBookings.size > 5000) {
      const oldSize = processedBookings.size;
      processedBookings = new Set([...processedBookings].slice(-2500));
      console.log(`🧹 Cleaned cache: ${oldSize} → ${processedBookings.size}`);
    }
    
    // Force GC if memory is high
    if (global.gc && memMB > 100) {
      const before = memUsage.heapUsed;
      global.gc();
      const after = process.memoryUsage().heapUsed;
      console.log(`🗑️ GC: ${Math.round((before - after) / 1024 / 1024)}MB freed`);
    }
    
  }, 20 * 60 * 1000);

  // Health check - restart if unhealthy for too long
  let unhealthyCount = 0;
  setInterval(() => {
    if (isShuttingDown) return;
    
    if (connectionStatus !== 'connected') {
      unhealthyCount++;
      console.log(`⚠️ Unhealthy for ${unhealthyCount} intervals (status: ${connectionStatus})`);
      
      // Force restart if unhealthy for 10 minutes
      if (unhealthyCount >= 10) {
        console.error('💥 Worker unhealthy for too long, forcing restart...');
        process.exit(1);
      }
    } else {
      unhealthyCount = 0;
    }
  }, 60 * 1000);
}

// Graceful shutdown handling
async function gracefulShutdown(signal) {
  console.log(`🛑 Received ${signal} - Starting graceful shutdown...`);
  isShuttingDown = true;
  connectionStatus = 'shutting_down';
  
  try {
    // Stop accepting new connections
    server.close();
    
    // Clean up Firebase presence
    const presenceRef = ref(rtdb, `workers/${WORKER_ID}`);
    await remove(presenceRef);
    console.log('🧹 Cleaned up worker presence');
    
    console.log('✅ Graceful shutdown complete');
    process.exit(0);
    
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
}

// Signal handlers
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Error handlers
process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error);
  errorCount++;
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection:', reason);
  errorCount++;
});

// Initialize everything
console.log('🚀 Starting robust monitoring systems...');
startRobustMonitoring();

console.log('🔥 Connecting to Firebase...');
initializeFirebaseConnection();

console.log('✅ Robust Firebase Worker Active - Zero Cold Start, 24/7 Monitoring');
console.log('🎯 Watching for Complete/Cancelled bookings with auto-recovery');