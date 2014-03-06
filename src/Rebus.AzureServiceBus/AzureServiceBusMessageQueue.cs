﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Rebus.Logging;
using Rebus.Shared;
using Timer = System.Timers.Timer;

namespace Rebus.AzureServiceBus
{
    public class AzureServiceBusMessageQueue : IDuplexTransport, IDisposable
    {
        static ILog log;

        static AzureServiceBusMessageQueue()
        {
            RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
        }

        public const string AzureServiceBusRenewLeaseAction = "AzureServiceBusRenewLeaseAction (invoke in order to renew the peek lock on the current message)";

        const string AzureServiceBusMessageBatch = "AzureServiceBusMessageBatch";

        const string AzureServiceBusReceivedMessage = "AzureServiceBusReceivedMessage";

        const string AzureServiceBusReceivedMessagePeekLockRenewalTimer = "AzureServiceBusReceivedMessagePeekLockRenewalTimer";

        const string AzureServiceBusReceivedMessagePeekLockRenewedTime = "AzureServiceBusReceivedMessagePeekLockRenewedTime";

        const string CurrentlyRenewingThePeekLockItemKey = "currently-renewing-the-peek-lock";

        /// <summary>
        /// Will be used to cache queue clients for each queue that we need to communicate with
        /// </summary>
        readonly ConcurrentDictionary<string, QueueClientContext> queueClientContexts = new ConcurrentDictionary<string, QueueClientContext>();
        readonly ConcurrentDictionary<string, WorkingMessageContext> innerWorkingQueue = new ConcurrentDictionary<string, WorkingMessageContext>();
        readonly NamespaceManager namespaceManager;
        readonly string connectionString;

        TimeSpan peekLockRenewalInterval = TimeSpan.FromMinutes(4.5);

        bool disposed;

        /// <summary>
        /// Construct a send-only instance of the transport
        /// </summary>
        public static AzureServiceBusMessageQueue Sender(string connectionString)
        {
            return new AzureServiceBusMessageQueue(connectionString, null);
        }

        public AzureServiceBusMessageQueue(string connectionString, string inputQueue)
        {
            this.connectionString = connectionString;
            try
            {
                log.Info("Initializing Azure Service Bus transport with input queue '{0}'", inputQueue);

                namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

                InputQueue = inputQueue;

                // if we're in one-way mode, just quit here
                if (inputQueue == null) return;

                EnsureQueueExists(inputQueue);
            }
            catch (Exception e)
            {
                throw new ApplicationException(
                    string.Format(
                        "An error occurred while initializing Azure Service Bus with input queue '{0}'",
                        inputQueue), e);
            }
        }

        public void EnsureQueueExists(string queueName)
        {
            try
            {
                if (namespaceManager.QueueExists(queueName)) return;
            }
            catch (TimeoutException exception)
            {
                log.Warn("An attempt to check whether ASB queue '{0}' exists timed out: {1} - will wait a short while and try again...",
                    queueName, exception);

                Thread.Sleep(TimeSpan.FromSeconds(5));

                if (namespaceManager.QueueExists(queueName)) return;
            }

            try
            {
                var lockDuration = TimeSpan.FromMinutes(5);
                const int maxDeliveryCount = 1000;

                log.Info("Queue '{0}' does not exist - it will be created now (with lockDuration={1} and maxDeliveryCount={2})",
                    queueName, lockDuration, maxDeliveryCount);

                namespaceManager.CreateQueue(new QueueDescription(queueName)
                {
                    LockDuration = lockDuration,
                    MaxDeliveryCount = maxDeliveryCount,
                    UserMetadata = string.Format("Queue created by Rebus {0}", DateTime.Now)
                });
            }
            catch
            {
                // just assume the call failed because the queue already exists
            }
        }

        public void Send(string destinationQueueName, TransportMessageToSend message, ITransactionContext context)
        {
            if (!context.IsTransactional)
            {
                var envelopeToSendImmediately = CreateEnvelope(message);

                var backoffTimes = new[] { 1, 2, 5, 10, 10, 10, 10, 10, 20, 20, 20, 30, 30, 30, 30 }
                    .Select(seconds => TimeSpan.FromSeconds(seconds))
                    .ToArray();

                new Retrier(backoffTimes)
                    .RetryOn<ServerBusyException>()
                    .RetryOn<MessagingCommunicationException>()
                    .RetryOn<TimeoutException>()
                    .TolerateInnerExceptionsAsWell()
                    .Do(() =>
                        {
                            using (var messageToSendImmediately = CreateBrokeredMessage(envelopeToSendImmediately))
                            {
                                GetClientContextFor(destinationQueueName)
                                    .Client
                                    .Send(messageToSendImmediately);
                            }
                        });

                return;
            }

            // if the batch is null, we're doing tx send outside of a message handler - this means
            // that we must initialize the collection of messages to be sent
            if (context[AzureServiceBusMessageBatch] == null)
            {
                context[AzureServiceBusMessageBatch] = new List<Tuple<string, Envelope>>();
                context.DoCommit += () => DoCommit(context);
            }

            var envelope = CreateEnvelope(message);

            var messagesToSend = (List<Tuple<string, Envelope>>)context[AzureServiceBusMessageBatch];

            messagesToSend.Add(Tuple.Create(destinationQueueName, envelope));
        }

        QueueClientContext GetClientContextFor(string destinationQueueName)
        {
            var clientContext = queueClientContexts.GetOrAdd(destinationQueueName, CreateNewClientContext);
            var client = clientContext.Client;

            if (!client.IsClosed)
                return clientContext;

            clientContext = CreateNewClientContext(destinationQueueName);
            queueClientContexts[destinationQueueName] = clientContext;
            return clientContext;
        }

        QueueClientContext CreateNewClientContext(string queueName)
        {
            var client = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);
            return new QueueClientContext(client, queueName);
        }

        void EnsureClientConfiguredForReceiveEvents(QueueClientContext clientContext)
        {
            if (clientContext.IsConfiguredForReceiveEvents)
                return;

            lock (clientContext.Client)
            {
                if (clientContext.IsConfiguredForReceiveEvents)
                    return;

                clientContext.IsConfiguredForReceiveEvents = true;
            }

            var client = clientContext.Client;
            var queueName = clientContext.QueueName;

            client.OnMessageAsync(message =>
            {
                Trace.WriteLine(string.Format("Received message: {0}", message.MessageId));

                var context = SetCurrentMessageContext(queueName, message);
                if (context != null)
                    return context.Handle.Task.ContinueWith(task => Trace.WriteLine(string.Format("Task complete: {0}", context.CorrelationId)));

                message.Abandon();
                var abortTask = new TaskCompletionSource<bool>();
                abortTask.SetCanceled();
                return abortTask.Task;
            },
            new OnMessageOptions { AutoComplete = false, MaxConcurrentCalls = 1 });
        }

        public ReceivedTransportMessage ReceiveMessage(ITransactionContext context)
        {
            try
            {
                CompletePreviousMessage(InputQueue);

                var clientContext = GetClientContextFor(InputQueue);
                EnsureClientConfiguredForReceiveEvents(clientContext);

                var messageContext = TryGetMessageFromWorkingQueue(InputQueue);

                if (messageContext == null)
                    return null;

                var brokeredMessage = messageContext.Message;

                if (brokeredMessage == null)
                    return null;

                Trace.WriteLine(string.Format("Rebus Message Id: {0}", brokeredMessage.MessageId));
                messageContext.Processed = true;
                var messageId = brokeredMessage.MessageId;

                try
                {
                    if (context.IsTransactional)
                    {
                        if (context[AzureServiceBusMessageBatch] != null)
                        {
                            throw new InvalidOperationException(
                                @"Attempted to receive message within transaction where one or more messages were already sent - that cannot be done, sorry!");
                        }

                        context[AzureServiceBusReceivedMessage] = brokeredMessage;
                        context[AzureServiceBusMessageBatch] = new List<Tuple<string, Envelope>>();

                        // inject method into message context to allow for long-running message handling operations to have their lock renewed
                        var peekLockRenewalAction = (Action)(() => RenewPeekLock(context, messageId));

                        context[AzureServiceBusReceivedMessagePeekLockRenewedTime] = DateTime.UtcNow;
                        context[AzureServiceBusRenewLeaseAction] = peekLockRenewalAction;

                        if (peekLockRenewalInterval > TimeSpan.FromSeconds(1))
                        {
                            context[AzureServiceBusReceivedMessagePeekLockRenewalTimer] =
                                ScheduleInvocation(peekLockRenewalAction, peekLockRenewalInterval);
                        }

                        context.DoCommit += () => DoCommit(context);
                        context.DoRollback += () => DoRollBack(context);
                        context.Cleanup += () => DoCleanUp(context);
                    }

                    try
                    {
                        var envelope = brokeredMessage.GetBody<Envelope>();

                        return CreateReceivedTransportMessage(messageId, envelope);
                    }
                    finally
                    {
                        if (!context.IsTransactional)
                        {
                            brokeredMessage.Complete();
                            brokeredMessage.Dispose();
                        }
                    }
                }
                catch (Exception receiveException)
                {
                    var message = string.Format("An exception occurred while handling brokered message {0}", messageId);

                    try
                    {
                        log.Info("Will attempt to abandon message {0}", messageId);
                        brokeredMessage.Abandon();
                    }
                    catch
                    {
                    }

                    throw new ApplicationException(message, receiveException);
                }
            }
            catch (TimeoutException)
            {
                return null;
            }
            catch (CommunicationObjectFaultedException)
            {
                return null;
            }
            catch (MessagingCommunicationException e)
            {
                if (!e.IsTransient)
                {
                    log.Warn("Caught exception while receiving message from queue '{0}': {1}", InputQueue, e);
                }

                return null;
            }
            catch (Exception e)
            {
                log.Warn("Caught exception while receiving message from queue '{0}': {1}", InputQueue, e);

                return null;
            }
        }

        void CompletePreviousMessage(string inputQueue)
        {
            WorkingMessageContext messageContext;
            if (!innerWorkingQueue.TryGetValue(inputQueue, out messageContext))
                return;

            if (!messageContext.Processed)
                return;

            if (!innerWorkingQueue.TryRemove(inputQueue, out messageContext))
                return;

            Trace.WriteLine(string.Format("Marking context complete: {0}", messageContext.CorrelationId));
            messageContext.Handle.SetResult(true);
        }

        WorkingMessageContext TryGetMessageFromWorkingQueue(string inputQueue)
        {
            WorkingMessageContext messageContext;
            if (!innerWorkingQueue.TryGetValue(inputQueue, out messageContext))
                return null;

            Trace.WriteLine(string.Format("Retrieved Context from working queue: {0}", messageContext.CorrelationId));
            return messageContext;
        }

        WorkingMessageContext SetCurrentMessageContext(string inputQueue, BrokeredMessage message)
        {
            var context = new WorkingMessageContext();
            var result = innerWorkingQueue.TryAdd(inputQueue, context);
            if (!result)
                return null;

            context.CorrelationId = Guid.NewGuid();
            context.Message = message;
            context.Processed = false;
            context.Handle = new TaskCompletionSource<bool>();

            Trace.WriteLine(string.Format("Set current message context: {0}", context.CorrelationId));

            return context;
        }

        void RenewPeekLock(ITransactionContext context, string messageId)
        {
            if (context[CurrentlyRenewingThePeekLockItemKey] != null) return;

            try
            {
                context[CurrentlyRenewingThePeekLockItemKey] = true;

                var peekLockRenewalBackoffTimes = new[] { 1, 2, 5, 10, 10, 10 }
                    .Select(s => TimeSpan.FromSeconds(s))
                    .ToArray();

                var messageToRenew = (BrokeredMessage)context[AzureServiceBusReceivedMessage];

                try
                {
                    new Retrier(peekLockRenewalBackoffTimes)
                        .RetryOn<Exception>()
                        .TolerateInnerExceptionsAsWell()
                        .DoNotRetryOn<MessageLockLostException>()
                        .OnRetryException(
                            (exception, delay, faultNumber) =>
                                log.Warn(
                                    "Attempt no. {0} to renew the peek lock on message {1} failed: {2} - will wait {3} before trying again",
                                    faultNumber, messageId, exception, delay))
                        .Do(messageToRenew.RenewLock);

                    context[AzureServiceBusReceivedMessagePeekLockRenewedTime] = DateTime.UtcNow;

                    log.Info("Peek lock renewed on message {0} - current lease is {1:0.0} seconds old", messageId,
                        TimeSinceLastLockRenewal(context).TotalSeconds);
                }
                catch (Exception exception)
                {
                    log.Warn("Could not renew peek lock on message {0}: {1}", messageId, exception);
                }
            }
            finally
            {
                context[CurrentlyRenewingThePeekLockItemKey] = null;
            }
        }

        TimeSpan TimeSinceLastLockRenewal(ITransactionContext context)
        {
            var timeOfLastRenewal = (DateTime)context[AzureServiceBusReceivedMessagePeekLockRenewedTime];
            var timeSinceLastRenewal = DateTime.UtcNow - timeOfLastRenewal;
            return timeSinceLastRenewal;
        }

        Timer ScheduleInvocation(Action actionToCall, TimeSpan interval)
        {
            var timer = new Timer(interval.TotalMilliseconds);
            timer.Elapsed += (o, ea) => actionToCall();
            timer.Start();

            return timer;
        }

        Envelope CreateEnvelope(TransportMessageToSend message)
        {
            return new Envelope
            {
                Body = message.Body,
                Headers = message.Headers != null
                    ? message.Headers.ToDictionary(h => h.Key, h => h.Value.ToString())
                    : null,
                Label = message.Label,
            };
        }

        ReceivedTransportMessage CreateReceivedTransportMessage(string messageId, Envelope envelope)
        {
            return new ReceivedTransportMessage
            {
                Id = messageId,
                Headers = envelope.Headers == null
                    ? new Dictionary<string, object>()
                    : envelope.Headers.ToDictionary(e => e.Key, e => (object)e.Value),
                Body = envelope.Body,
                Label = envelope.Label
            };
        }

        void DoRollBack(ITransactionContext context)
        {
            try
            {
                var brokeredMessage = (BrokeredMessage)context[AzureServiceBusReceivedMessage];

                brokeredMessage.Abandon();
            }
            catch
            {
            }
        }

        void DoCleanUp(ITransactionContext context)
        {
            var timer = context[AzureServiceBusReceivedMessagePeekLockRenewalTimer] as Timer;
            if (timer != null)
            {
                timer.Stop();
                timer.Dispose();
            }

            try
            {
                var brokeredMessage = (BrokeredMessage)context[AzureServiceBusReceivedMessage];

                brokeredMessage.Dispose();
            }
            catch
            {
            }
        }

        void DoCommit(ITransactionContext context)
        {
            // the message will be null when doing tx send outside of a message handler
            var stuffToDispose = new List<IDisposable>();
            var receivedMessageOrNull = context[AzureServiceBusReceivedMessage] as BrokeredMessage;
            var messagesToSend = (List<Tuple<string, Envelope>>)context[AzureServiceBusMessageBatch];

            try
            {
                var backoffTimes = new[] { 1, 2, 5, 10, 10, 10 }
                    .Select(seconds => TimeSpan.FromSeconds(seconds))
                    .ToArray();

                if (messagesToSend.Any())
                {
                    var messagesForEachRecipient = messagesToSend
                        .GroupBy(g => g.Item1)
                        .ToList();

                    foreach (var group in messagesForEachRecipient)
                    {
                        var destinationQueueName = group.Key;
                        var messagesForThisRecipient = group.Select(g => g.Item2).ToList();

                        const int batchThreshold = 100;
                        if (messagesForThisRecipient.Count < batchThreshold)
                        {
                            log.Debug("Less than {0} messages to be sent to {1} - will perform one single send operation for each message",
                                batchThreshold, destinationQueueName);
                            
                            messagesForThisRecipient.ForEach(message =>
                            {
                                var brokeredMessage = CreateBrokeredMessage(message);
                                stuffToDispose.Add(brokeredMessage);

                                new Retrier(backoffTimes)
                                    .RetryOn<ServerBusyException>()
                                    .RetryOn<MessagingCommunicationException>()
                                    .RetryOn<TimeoutException>()
                                    .TolerateInnerExceptionsAsWell()
                                    .OnRetryException(
                                        (exception, delay, faultNumber) =>
                                            log.Warn("An exception occurred while making attempt no. {0} to send message from batch of {1} to {2}: {3} - will wait {4} and try again",
                                                faultNumber, messagesToSend.Count, destinationQueueName, exception, delay))
                                    .Do(() => GetClientContextFor(destinationQueueName).Client.Send(brokeredMessage));
                            });
                        }
                        else
                        {
                            var messageBatches = messagesForThisRecipient.Partition(100).ToList();

                            log.Debug("More than {0} messages to be sent to {1} - will send messages in {2} batches",
                                batchThreshold, destinationQueueName, messageBatches.Count);

                            foreach (var batch in messageBatches)
                            {
                                var brokeredMessagesInThisBatch = batch
                                    .Select(CreateBrokeredMessage)
                                    .ToList();

                                stuffToDispose.AddRange(brokeredMessagesInThisBatch);

                                new Retrier(backoffTimes)
                                    .RetryOn<ServerBusyException>()
                                    .RetryOn<MessagingCommunicationException>()
                                    .RetryOn<TimeoutException>()
                                    .TolerateInnerExceptionsAsWell()
                                    .OnRetryException(
                                        (exception, delay, faultNumber) =>
                                            log.Warn(
                                                "An exception occurred while making attempt no. {0} to send batch of {1} messages to {2} (out of a total of {3} messages): {4} - will wait {5} and try again",
                                                faultNumber, brokeredMessagesInThisBatch.Count, destinationQueueName,
                                                messagesToSend.Count, exception, delay))
                                    .Do(() => GetClientContextFor(destinationQueueName).Client.SendBatch(brokeredMessagesInThisBatch));
                            }
                        }
                    }
                }

                if (receivedMessageOrNull != null)
                {
                    receivedMessageOrNull.Complete();
                }
            }
            catch (Exception)
            {
                try
                {
                    if (receivedMessageOrNull != null)
                    {
                        receivedMessageOrNull.Abandon();
                    }
                }
                catch (Exception rollbackException)
                {
                    log.Warn("An exception occurred while attempting to roll back: {0}", rollbackException);
                }

                throw;
            }
            finally
            {
                stuffToDispose.ForEach(d => d.Dispose());
            }
        }

        BrokeredMessage CreateBrokeredMessage(Envelope envelope)
        {
            var brokeredMessage = new BrokeredMessage(envelope);

            if (envelope.Headers.ContainsKey(Headers.MessageId))
            {
                brokeredMessage.MessageId = envelope.Headers[Headers.MessageId];
            }

            if (envelope.Headers.ContainsKey(Headers.CorrelationId))
            {
                brokeredMessage.CorrelationId = envelope.Headers[Headers.CorrelationId];
            }

            if (envelope.Headers.ContainsKey(Headers.ReturnAddress))
            {
                brokeredMessage.ReplyTo = envelope.Headers[Headers.ReturnAddress];
            }

            brokeredMessage.Label = envelope.Label;

            return brokeredMessage;
        }

        public string InputQueue { get; private set; }

        public string InputQueueAddress { get { return InputQueue; } }

        public AzureServiceBusMessageQueue Purge()
        {
            log.Warn("Purging queue '{0}'", InputQueue);

            namespaceManager.DeleteQueue(InputQueue);
            EnsureQueueExists(InputQueue);

            return this;
        }

        public AzureServiceBusMessageQueue Delete()
        {
            log.Warn("Deleting queue '{0}'", InputQueue);

            namespaceManager.DeleteQueue(InputQueue);

            return this;
        }

        public void SetAutomaticPeekLockRenewalInterval(TimeSpan peekLockRenewalTimeSpan)
        {
            peekLockRenewalInterval = peekLockRenewalTimeSpan;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;

            if (disposing)
            {
                try
                {
                    foreach (var clientContext in queueClientContexts)
                    {
                        log.Info("Closing queue client for '{0}'", clientContext.Key);

                        clientContext.Value.Client.Close();
                    }
                }
                catch (Exception e)
                {
                    log.Warn("An exception occurred while closing queue client(s): {0}", e);
                }
            }

            disposed = true;
        }

        private class WorkingMessageContext
        {
            public Guid CorrelationId { get; set; }
            public BrokeredMessage Message { get; set; }
            public TaskCompletionSource<bool> Handle { get; set; }
            public bool Processed { get; set; }
        }

        private class QueueClientContext
        {
            public QueueClientContext(QueueClient client, string queueName)
            {
                Client = client;
                QueueName = queueName;
            }

            public QueueClient Client { get; private set; }
            public string QueueName { get; private set; }
            public bool IsConfiguredForReceiveEvents { get; set; }
        }
    }
}