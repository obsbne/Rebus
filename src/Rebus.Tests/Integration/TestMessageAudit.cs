﻿using System;
using System.Collections.Generic;
using System.Messaging;
using System.Threading;
using NUnit.Framework;
using Rebus.Configuration;
using Rebus.Serialization.Json;
using Rebus.Shared;
using Rebus.Transports.Msmq;
using Shouldly;
using Message = Rebus.Messages.Message;

namespace Rebus.Tests.Integration
{
    [TestFixture]
    public class TestMessageAudit : FixtureBase
    {
        const string InputQueueName = "test.audit.input";
        const string AuditQueueName = "test.audit.audit";
        List<IDisposable> disposables;
        BuiltinContainerAdapter adapter;

        protected override void DoSetUp()
        {
            adapter = new BuiltinContainerAdapter();
            disposables = new List<IDisposable> {adapter};

            Configure.With(adapter)
                .Transport(t => t.UseMsmq(InputQueueName, "error"))
                .Behavior(b => b.EnableMessageAudit(AuditQueueName))
                .CreateBus()
                .Start(1);

            MsmqUtil.EnsureMessageQueueExists(MsmqUtil.GetPath(AuditQueueName));
            MsmqUtil.PurgeQueue(AuditQueueName);
        }

        static string InputQueueAddress
        {
            get { return InputQueueName + "@" + Environment.MachineName; }
        }

        [Test]
        public void CanCopySuccessfullyHandledMessageToAuditQueue()
        {
            // arrange
            var fakeTime = DateTime.UtcNow;
            TimeMachine.FixTo(fakeTime);
            var resetEvent = new ManualResetEvent(false);
            adapter.Handle<string>(str => resetEvent.Set());

            // act
            adapter.Bus.SendLocal("yo!");
            
            // assert
            var message = GetMessageFrom(AuditQueueName);
            
            message.ShouldNotBe(null);

            var logicalMessages = message.Messages;
            var headers = message.Headers;

            logicalMessages.Length.ShouldBe(1);
            logicalMessages[0].ShouldBe("yo!");

            headers.ShouldContainKeyAndValue(Headers.AuditReason, Headers.AuditReasons.Handled);
            headers.ShouldContainKeyAndValue(Headers.AuditMessageCopyTime, fakeTime.ToString("u"));
            headers.ShouldContainKeyAndValue(Headers.AuditSourceQueue, InputQueueAddress);
        }

        [Test]
        public void CanCopyPublishedMessageToAuditQueue()
        {
            // arrange
            var fakeTime = DateTime.UtcNow;
            TimeMachine.FixTo(fakeTime);

            // act
            adapter.Bus.Publish("yo!");
            
            // assert
            var message = GetMessageFrom(AuditQueueName);
            
            message.ShouldNotBe(null);

            var logicalMessages = message.Messages;
            var headers = message.Headers;

            logicalMessages.Length.ShouldBe(1);
            logicalMessages[0].ShouldBe("yo!");

            headers.ShouldContainKeyAndValue(Headers.AuditReason, Headers.AuditReasons.Published);
            headers.ShouldContainKeyAndValue(Headers.AuditMessageCopyTime, fakeTime.ToString("u"));
            headers.ShouldContainKeyAndValue(Headers.AuditSourceQueue, InputQueueAddress);
        }

        protected override void DoTearDown()
        {
            disposables.ForEach(d => d.Dispose());

            DeleteQueue(InputQueueName);
            DeleteQueue(AuditQueueName);
        }

        Message GetMessageFrom(string queueName)
        {
            using (var queue = new MessageQueue(MsmqUtil.GetPath(queueName)))
            {
                queue.Formatter = new RebusTransportMessageFormatter();
                queue.MessageReadPropertyFilter = RebusTransportMessageFormatter.PropertyFilter;

                try
                {
                    var receivedTransportMessage = (ReceivedTransportMessage) queue.Receive(3.Seconds()).Body;
                    var serializer = new JsonMessageSerializer();

                    return serializer.Deserialize(receivedTransportMessage);
                }
                catch (MessageQueueException exception)
                {
                    if (exception.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                    {
                        return null;
                    }

                    throw;
                }
            }
        }

        static void DeleteQueue(string queueName)
        {
            if (!MsmqUtil.QueueExists(queueName)) return;

            MsmqUtil.Delete(queueName);
        }
    }
}