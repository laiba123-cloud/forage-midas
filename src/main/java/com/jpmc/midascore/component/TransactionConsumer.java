package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
  public class TransactionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final UserRepository userRepository;

    public TransactionConsumer(UserRepository userRepository) {
              this.userRepository = userRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
        public void listen(Transaction transaction) {
                  logger.info("Received transaction: {}", transaction);

            UserRecord sender = userRepository.findById(transaction.getSenderId());
                  UserRecord recipient = userRepository.findById(transaction.getRecipientId());

            if (sender == null || recipient == null) {
                          logger.warn("Invalid transaction - sender or recipient not found: {}", transaction);
                          return;
            }

            if (sender.getBalance() < transaction.getAmount()) {
                          logger.warn("Insufficient funds for sender id={}, balance={}, amount={}",
                                                          sender.getId(), sender.getBalance(), transaction.getAmount());
                          return;
            }

            sender.setBalance(sender.getBalance() - transaction.getAmount());
                  recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            userRepository.save(sender);
                  userRepository.save(recipient);

            logger.info("Transaction processed: sender id={} new balance={}, recipient id={} new balance={}",
                                        sender.getId(), sender.getBalance(), recipient.getId(), recipient.getBalance());
        }
  }
