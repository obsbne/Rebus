﻿using System;
using System.Threading;
using NUnit.Framework;
using Rebus.Bus;
using Rebus.Configuration;
using Rebus.Serialization.Json;
using Rebus.Tests.Transports.Rabbit;
using Rebus.RabbitMQ;
using Shouldly;

namespace Rebus.Tests.Bugs
{
    [TestFixture, Category(TestCategories.Rabbit)]
    public class DoesNotCreateAndBindErrorQueueTooEarlyWithRabbitMq : RabbitMqFixtureBase
    {
        [Test]
        public void ItHasBeenFixed()
        {
            using (var adapter = new BuiltinContainerAdapter())
            {
                adapter.Handle<string>(s =>
                    {
                        throw new ApplicationException("Will trigger that the message gets moved to the error queue");
                    });

                Configure.With(adapter)
                         .Transport(t => t.UseRabbitMq(ConnectionString, "test.input", "test.error")
                                          .UseExchange("AlternativeExchange"))
                         .CreateBus()
                         .Start();

                adapter.Bus.SendLocal("hello there!!!");

                // wait until we're sure
                Thread.Sleep(2.Seconds());

                using (var errorQueue = new RabbitMqMessageQueue(ConnectionString, "test.error"))
                {
                    var serializer = new JsonMessageSerializer();
                    var receivedTransportMessage = errorQueue.ReceiveMessage(new NoTransaction());
                    receivedTransportMessage.ShouldNotBe(null);

                    var errorMessage = serializer.Deserialize(receivedTransportMessage);
                    errorMessage.Messages.Length.ShouldBe(1);
                    errorMessage.Messages[0].ShouldBeOfType<string>();
                    ((string)errorMessage.Messages[0]).ShouldBe("hello there!!!");
                }
            }
        }
    }
}