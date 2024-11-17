
import java.sql.Timestamp


sealed case class ChannelExtract(
                          guild: Guild,
                          channel: Channel,
//                          dateRange: DateRange,
                          exportedAt: Timestamp,
                          messages: List[Message],
                          messageCount: Long
                        )

sealed case class Guild (
                          id: String,
                          name: String,
                          iconUrl: String
                        )
case class Channel(
                    id: String,
//                    `type`: String,
//                    categoryId: Option[String],
//                    category: Option[String],
                    name: String
//                    topic: Option[String]
                  )
sealed case class DateRange(
                            after: Timestamp,
                            before: Timestamp
                            )


sealed case class User(
                      id: String,
                      name: String,
//                      discriminator: String,
                      nickname: String,
                      isBot: Boolean,
                      avatarURL: String
                      )
//
//sealed case class Role(
//                      id: String,
//                      name: String,
//                      color: String,
//                      position: Long
//                      )

sealed case class Message(
                         id: String,
                         `type`: String,
                         timestamp: Timestamp,
//                         timestampEdited: Timestamp,
//                         callEndedTimestamp: Timestamp,
                         isPinned: Boolean,
                         content: String,
                         author: User,
//                         attachments: List[String],
//                         embeds: List[String],
//                         stickers: List[String],
                         reactions: List[Reaction]
                         )
sealed case class Reaction (
                            emoji: Emoji,
                            count: Long,
                            users: List[User]
                           )
sealed case class Emoji(
                       id: String,
                       name: String,
//                       code: String,
//                       isAnimated: Boolean,
                       imageUrl: String
                       )