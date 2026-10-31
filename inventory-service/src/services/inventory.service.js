const prisma = require('../config/prisma');
const logger = require('../config/logger');
const inventoryProducer = require('../kafka/producer/inventory.producer');
const { retryTransaction } = require('../utils/retryTransaction');
const { BadRequestError, NotFoundError, ConflictError, ForbiddenError } = require('../utils/error');
const { config } = require('../config');

// ─── Kafka Event Handlers ───────────────────────────────────────────────────

const initializeInventory = async (eventData) => {
     const { scheduleId, trainId, trainNumber, trainName, departureDate, seats } = eventData;

     if (!scheduleId || !seats || !seats.length) {
          logger.warn('Invalid SCHEDULE_CREATED event — missing scheduleId or seats');
          return;
     }

     const eventKey = `SCHEDULE_CREATED:${scheduleId}`;

     const existing = await prisma.idempotencyRecord.findUnique({ where: { eventKey } });
     if (existing) {
          logger.info(`Duplicate event skipped: ${eventKey}`);
          return;
     }

     const totalSeats = seats.length;

     await prisma.$transaction(async (tx) => {
          const schedule = await tx.scheduleInventory.create({
               data: {
                    scheduleId,
                    trainId,
                    trainNumber,
                    trainName,
                    departureDate: new Date(departureDate),
                    totalSeats,
                    available: totalSeats,
                    locked: 0,
                    booked: 0,
                    status: 'ACTIVE',
               },
          });

          const seatData = seats.map(seat => ({
               scheduleInventoryId: schedule.id,
               scheduleId,
               seatId: seat.seatId,
               seatNumber: seat.seatNumber,
               seatType: seat.seatType,
               price: seat.price,
               status: 'AVAILABLE',
          }));

          await tx.seatInventory.createMany({ data: seatData });

          // --- SEGMENT BOOKING: Persist route topology for segment overlap checks ---
          if (eventData.route && eventData.route.length > 0) {
               const routeStopData = eventData.route.map(rs => ({
                    scheduleId,
                    stationId: rs.stationId,
                    stationName: rs.stationName,
                    stationCode: rs.stationCode,
                    sequenceNumber: rs.sequenceNumber,
               }));
               await tx.routeStop.createMany({ data: routeStopData });
               logger.info(`Persisted ${routeStopData.length} route stops for schedule ${scheduleId}`);
          }

          await tx.idempotencyRecord.create({ data: { eventKey } });
     });

     logger.info(`Inventory initialized for schedule ${scheduleId} with ${totalSeats} seats`);

     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(scheduleId, trainId, totalSeats, 0, 0);
     } catch (err) {
          logger.error('Failed to publish initial availability event after retries', { scheduleId, error: err.message });
     }
};

const cancelScheduleInventory = async (eventData) => {
     const data = eventData.data || eventData;
     const scheduleId = data.scheduleId || data.id;

     if (!scheduleId) {
          logger.warn('Invalid SCHEDULE_CANCELLED event — missing scheduleId');
          return;
     }

     const eventKey = `SCHEDULE_CANCELLED:${scheduleId}`;

     const existing = await prisma.idempotencyRecord.findUnique({ where: { eventKey } });
     if (existing) {
          logger.info(`Duplicate event skipped: ${eventKey}`);
          return;
     }

     const schedule = await prisma.scheduleInventory.findUnique({ where: { scheduleId } });
     if (!schedule) {
          logger.warn(`Schedule ${scheduleId} not found in inventory — skipping cancellation`);
          return;
     }

     await prisma.$transaction(async (tx) => {
          await tx.scheduleInventory.update({
               where: { scheduleId },
               data: { status: 'CANCELLED', available: 0, locked: 0, booked: 0, version: { increment: 1 } },
          });

          await tx.seatInventory.updateMany({
               where: { scheduleId },
               data: { status: 'CANCELLED' },
          });

          await tx.idempotencyRecord.create({ data: { eventKey } });
     });

     logger.info(`Inventory cancelled for schedule ${scheduleId}`);

     try {
          await inventoryProducer.publishSeatAvailabilityUpdated(scheduleId, schedule.trainId, 0, 0, 0);
     } catch (err) {
          logger.error('Failed to publish cancellation availability event after retries', { scheduleId, error: err.message });
     }
};

module.exports = {
     initializeInventory,
     cancelScheduleInventory
};
